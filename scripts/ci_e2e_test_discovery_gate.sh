#!/usr/bin/env bash
#
# bd-ohk1x: fail when an fsqlite-e2e integration test exists that no configured
# CI workflow covers.
#
# WHY THIS EXISTS
#
# `crates/fsqlite-e2e/tests/*.rs` holds the differential, conformance, and
# corruption suites. Historically, two independent mechanisms meant most of
# them never executed:
#
#   1. concurrent-platform-matrix.yml runs `cargo test -p fsqlite-e2e --no-run`
#      (build only), then executes an explicit `--test <name>` allowlist.
#   2. unit-test-shard-matrix.yml runs `cargo test -p "${crate}" --lib` for
#      every crate but fsqlite-cli. `fsqlite-e2e` is in that shard's crate list,
#      but `--lib` never touches `tests/`.
#
# A newer sharded workflow can instead declare
# `E2E_COVERAGE_MODE: all-tracked-targets`, enumerate the same tracked source of
# truth as this gate, execute each target, and account for exactly one successful
# receipt per target. When that structural contract is present, every tracked
# integration target is covered without a hand-maintained literal allowlist.
#
# WHAT THIS GATE DOES, AND DELIBERATELY DOES NOT DO
#
# It accepts either literal `--test <target>` workflow coverage or the
# all-tracked-targets contract above. Any remaining gaps are frozen in an
# explicit baseline, and the gate fails on drift:
#
#   * a test file that is neither named by a workflow nor listed in the baseline
#     -> NEW uncovered test. Fail: wire it in, or add it to the baseline with intent.
#   * a baseline entry that no longer exists on disk -> stale. Fail: clean it up.
#   * a baseline entry that a workflow now covers -> it is configured to run.
#     Fail: remove it so the baseline keeps meaning "not configured to run".
#
# This is deliberately a repository-static configuration check. It cannot prove
# that GitHub has enabled the workflow or that its event triggers match the
# repository's operating model. That live administrative/cadence gate is tracked
# separately by bd-xekq8 and remains required release evidence.
#
# The point is that the allowlist stops drifting silently. A drifting allowlist
# is worse than an honest small one, because it reads as coverage.
#
# Usage:
#   scripts/ci_e2e_test_discovery_gate.sh            # check (exit 1 on drift)
#   scripts/ci_e2e_test_discovery_gate.sh --write    # regenerate the baseline
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root" || exit 2

tests_dir="crates/fsqlite-e2e/tests"
workflows_dir=".github/workflows"
baseline=".github/e2e-tests-not-run-in-ci.txt"
mode="${1:-check}"

case "$mode" in
  check | --write) ;;
  *)
    echo "::error::unsupported mode '$mode' (expected no argument or --write)"
    exit 2
    ;;
esac

if [ ! -d "$tests_dir" ]; then
  echo "::error::$tests_dir not found (run from the repo, or the crate moved)"
  exit 2
fi
if [ ! -d "$workflows_dir" ]; then
  echo "::error::$workflows_dir not found"
  exit 2
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# Every integration-test target name in the crate.
#
# Enumerate TRACKED files, not whatever is on disk. A shared developer checkout
# routinely carries untracked scratch tests (e.g. the `zz_*` rch probes), and
# generating the baseline from `find` bakes those transient names in — then the
# gate fails on a clean CI checkout where they do not exist. Ask git instead, and
# fall back to `find` only outside a work tree (e.g. a source tarball).
if git -C . rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  git -C . ls-files "$tests_dir/*.rs" \
    | xargs -r -n1 basename \
    | sed 's/\.rs$//' | LC_ALL=C sort -u > "$tmp/all"
else
  find "$tests_dir" -maxdepth 1 -name '*.rs' -type f -printf '%f\n' \
    | sed 's/\.rs$//' | LC_ALL=C sort -u > "$tmp/all"
fi
if [ ! -s "$tmp/all" ]; then
  echo "::error::no tracked fsqlite-e2e integration targets were discovered"
  exit 1
fi

# Every test target any workflow names. `--test foo` and `--test=foo` both count.
# A repository may rely entirely on the dynamic all-target contract, so an
# empty literal match set is valid input rather than a pipefail-worthy error.
{
  grep -rhoE -- '--test[= ][A-Za-z0-9_-]+' "$workflows_dir" 2>/dev/null || true
} | sed -E 's/^--test[= ]//' > "$tmp/named_raw"

# A dynamic all-target workflow cannot expose literal target names to the grep
# above. Admit its coverage only when the same workflow file contains all three
# structural anchors: the explicit mode declaration, matching tracked-file
# enumeration in both execution and accounting jobs, the command that executes
# the loop's target, and the exact-accounting step. Runtime receipts in that
# workflow remain the authoritative proof that no target was skipped or
# duplicated.
dynamic_all_workflow=""
dynamic_match_count=0
for workflow in "$workflows_dir"/*.yml "$workflows_dir"/*.yaml; do
  [ -f "$workflow" ] || continue
  # Full-line comments are documentation, not executable workflow structure.
  # Strip them before matching so a commented-out contract cannot erase the
  # uncovered-test baseline.
  sed '/^[[:space:]]*#/d' "$workflow" > "$tmp/workflow_without_comments"
  tracked_enumeration_count="$(
    grep -Fc "git ls-files \"\${E2E_TEST_SOURCE_DIR}/*.rs\"" \
      "$tmp/workflow_without_comments" || true
  )"
  source_dir_declaration_count="$(
    grep -Fc 'E2E_TEST_SOURCE_DIR: crates/fsqlite-e2e/tests' \
      "$tmp/workflow_without_comments" || true
  )"
  if grep -Fq 'E2E_COVERAGE_MODE: all-tracked-targets' \
      "$tmp/workflow_without_comments" \
    && [ "$source_dir_declaration_count" -ge 2 ] \
    && [ "$tracked_enumeration_count" -ge 2 ] \
    && grep -Fq "cargo test --locked -p fsqlite-e2e --test \"\${target}\"" \
      "$tmp/workflow_without_comments" \
    && grep -Fq 'name: Upload E2E integration shard receipts' \
      "$tmp/workflow_without_comments" \
    && grep -Fq 'name: Download E2E integration shard receipts' \
      "$tmp/workflow_without_comments" \
    && grep -Fq 'name: Enforce exact E2E target accounting' \
      "$tmp/workflow_without_comments"; then
    dynamic_match_count=$((dynamic_match_count + 1))
    dynamic_all_workflow="$workflow"
    cat "$tmp/all" >> "$tmp/named_raw"
  fi
done
if [ "$dynamic_match_count" -gt 1 ]; then
  echo "::error::multiple workflows declare the dynamic all-target E2E contract"
  exit 1
fi
LC_ALL=C sort -u "$tmp/named_raw" > "$tmp/named"

LC_ALL=C comm -23 "$tmp/all" "$tmp/named" > "$tmp/unrun"

if [ "$mode" = "--write" ]; then
  {
    echo "# bd-ohk1x baseline: fsqlite-e2e tests no configured CI workflow covers."
    echo "#"
    echo "# Generated by scripts/ci_e2e_test_discovery_gate.sh --write"
    echo "# Do not hand-sort; regenerate instead."
    echo "#"
    echo "# Every name here compiles in CI but no configured workflow executes it. That is a coverage"
    echo "# gap, not an approval -- shrinking this file is the goal. Removing a"
    echo "# name requires actually wiring the test into a workflow."
    if [ -n "$dynamic_all_workflow" ]; then
      echo "# Dynamic all-target coverage: ${dynamic_all_workflow}"
      echo "# Live enablement and trigger cadence are not asserted here; see bd-xekq8."
    fi
    cat "$tmp/unrun"
  } > "$baseline"
  baseline_entry_count="$(awk '!/^#/ && !/^[[:space:]]*$/ { count++ } END { print count + 0 }' "$baseline")"
  echo "wrote $baseline (${baseline_entry_count} entries)"
  exit 0
fi

if [ ! -f "$baseline" ]; then
  echo "::error::$baseline is missing; regenerate with: $0 --write"
  exit 1
fi

awk '!/^#/ && !/^[[:space:]]*$/' "$baseline" | LC_ALL=C sort -u > "$tmp/baseline"

failed=0

# 1. New uncovered tests: tracked, not covered by a workflow contract, and not
# in the baseline.
LC_ALL=C comm -23 "$tmp/unrun" "$tmp/baseline" > "$tmp/new_unrun"
if [ -s "$tmp/new_unrun" ]; then
  failed=1
  count=$(wc -l < "$tmp/new_unrun")
  echo "::error::${count} fsqlite-e2e integration test(s) have no configured workflow coverage and are not in the baseline."
  echo "         They may compile without any configured execution receipt, so they gate nothing."
  while IFS= read -r t; do
    echo "::error file=${tests_dir}/${t}.rs::'${t}' has no configured workflow coverage. Add '--test ${t}' to a workflow, or add it to ${baseline} if that is intended."
  done < "$tmp/new_unrun"
fi

# 2. Stale baseline entries: listed as uncovered, but the file is gone.
LC_ALL=C comm -13 "$tmp/all" "$tmp/baseline" > "$tmp/ghosts"
if [ -s "$tmp/ghosts" ]; then
  failed=1
  while IFS= read -r t; do
    echo "::error file=${baseline}::'${t}' is in the baseline but ${tests_dir}/${t}.rs does not exist. Remove the stale entry."
  done < "$tmp/ghosts"
fi

# 3. Baseline entries now covered by configured CI: the baseline must keep
# meaning "no configured workflow coverage".
LC_ALL=C comm -12 "$tmp/baseline" "$tmp/named" > "$tmp/now_running"
if [ -s "$tmp/now_running" ]; then
  failed=1
  while IFS= read -r t; do
    echo "::error file=${baseline}::'${t}' now has configured workflow coverage. Remove it from the uncovered baseline."
  done < "$tmp/now_running"
fi

total=$(wc -l < "$tmp/all")
named=$(LC_ALL=C comm -12 "$tmp/all" "$tmp/named" | wc -l)
unrun=$(wc -l < "$tmp/unrun")
echo "fsqlite-e2e integration tests: ${total} total, ${named} configured for CI execution, ${unrun} uncovered."
if [ -n "$dynamic_all_workflow" ]; then
  echo "dynamic all-target coverage: ${dynamic_all_workflow}"
fi

if [ "$failed" -eq 0 ]; then
  echo "e2e test-discovery gate: OK (no drift against ${baseline})"
fi
exit "$failed"
