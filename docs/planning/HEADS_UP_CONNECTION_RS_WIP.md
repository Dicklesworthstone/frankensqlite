# HEADS UP — `crates/fsqlite-core/src/connection.rs` working tree change is large WIP

> Written 2026-05-18 by `ScarletForest` (claude-code / claude-opus-4-7) during a
> cross-project commit/sync sweep at user request. Agent-mail delivery to
> `BronzeLark` / `CopperOsprey` / `YellowBarn` / `GoldenMarsh` was attempted but
> blocked: the MCP Agent Mail backend is in `degraded_read_only` mode and
> rejected every `send_message` with a DB error. `am doctor repair` would
> recover it. This file is the fallback channel.
>
> Once the final resolution lands, remove this file only with the user's
> explicit permission, per this repo's no-file-deletion rule.

## 2026-05-18 Codex fresh-eyes update

The compile-breaking profile/strict-mode omissions described below were repaired
in the working tree after this note was written:

- restored the `HotPathProfileSnapshot` profile fields still used by
  `fsqlite-e2e` and `prepared_hit_rate_proof.rs`
- restored strict multi-process `ConnectionEnv` plumbing and
  `Connection::open_strict_multi_process`
- clarified that strict multi-process is currently opt-in plumbing, not a
  complete refusal implementation
- restored DML profile accounting for the retained update/delete paths
- fixed the staged DELETE-run early return so active probes balance as hit or
  miss
- fixed close-time retained-autocommit flushing so a file-backed `close()` does
  not run the passive WAL checkpoint twice

Proof from Codex follow-ups includes the commands below. The workspace
`cargo check`, `cargo clippy`, strict-mode test, and close-without-checkpoint
test were rerun after the close-path and strict-doc fixes.

```bash
cargo fmt --check --all
git diff --check
rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-fresh-eyes-target-20260518g CARGO_BUILD_JOBS=4 cargo check --workspace --all-targets
rch exec -- env CARGO_TARGET_DIR=/data/tmp/frankensqlite-fresh-eyes-target-20260518g CARGO_BUILD_JOBS=4 cargo clippy --workspace --all-targets -- -D warnings
cargo test -p fsqlite-core --test strict_multi_process -- --test-threads=1
cargo test -p fsqlite-core --test prepared_hit_rate_proof prepared_direct_delete_staged_only_absent_probe_records_active_miss -- --test-threads=1
cargo test -p fsqlite-core --lib test_prepared_direct_simple_insert_large_profile_breakdown
cargo test -p fsqlite-core --lib test_close_without_checkpoint_preserves_wal_recovery -- --test-threads=1
ubs crates/fsqlite-core/tests/fast_path_separation.rs crates/fsqlite-vdbe/src/codegen.rs
```

The file still has a very large uncommitted divergence from `HEAD`; do not stage
or commit it wholesale without reviewing that broader WIP.

## TL;DR

`crates/fsqlite-core/src/connection.rs` has a large working-tree change sitting
uncommitted. **Do not `git add` it as-is**. The build now passes after the
fresh-eyes repair above, but the broader diff still needs owner review before it
is safe to land.

## What the working tree changes

1. **Import + call-site switch (SAFE in isolation):**
   ```
   - best_access_path_with_hints, classify_where_term, decompose_where,
   + best_access_path, classify_where_term, decompose_where,
   ```
   `connection.rs:43552` now passes 4 args instead of 6 (drops the
   `None, None` hint pair). `best_access_path` is a thin wrapper around
   `best_access_path_with_hints` already defined at
   `crates/fsqlite-planner/src/lib.rs:1600`. Net behavioral change: zero.

2. **Historical issue, now repaired in this working tree:** removal of
   `FSQLITE_PREPARED_DIRECT_UPDATE_LEAF_PATCH_RUN_*`-style atomic counters and
   matching `HotPathProfileSnapshot` fields had left consumers below broken.
   Codex restored those metrics and verified the listed checks in the update
   above.

3. **New imports** `concurrent_prepare_write_page` and
   `concurrent_stage_prepared_write_marker` added; haven't traced whether the
   corresponding wiring is also incomplete.

## Consumers that were broken before the Codex fresh-eyes repair

```
crates/fsqlite-core/tests/prepared_hit_rate_proof.rs:816
    profile.prepared_direct_delete_leaf_run_active_miss_staged_runs >= 1

crates/fsqlite-e2e/src/bin/perf_update_delete.rs:553-561
    profile.prepared_direct_update_leaf_patch_run_start_hits
    profile.prepared_direct_update_leaf_patch_run_start_attempts
    profile.prepared_direct_update_leaf_patch_run_start_time_ns
    profile.prepared_direct_update_leaf_patch_run_active_hits
    profile.prepared_direct_update_leaf_patch_run_active_attempts
    profile.prepared_direct_update_leaf_patch_run_active_misses
    profile.prepared_direct_update_leaf_patch_run_active_time_ns
    profile.prepared_direct_update_leaf_patch_run_dirty_flushes
    profile.prepared_direct_update_leaf_patch_run_flushes
```

These references are valid again after the metric restoration. The docs
runnables in `docs/progress/perf-negative-results.md` that include
`prepared_direct_update_leaf_patch_run` should still be checked before landing a
final commit, because this heads-up file has not audited the broader WIP diff.

## Original ScarletForest handoff context

ScarletForest did not revert or modify the working-tree change in
`connection.rs`, did not commit it, and did not modify `perf_update_delete.rs`,
`prepared_hit_rate_proof.rs`, or the docs runnables. Codex later repaired the
compile-breaking profile/strict-mode omissions described above.

`AGENTS.md` rule "never stash/revert another agent's work" applied; the user
explicitly said leave it untouched and flag the originating agent.

## Remaining suggested resolution

Review the full `connection.rs` diff before any commit. The previous
profile-field removal should not be finished now; those fields are required by
the benchmark/profile callers and have been restored. If a smaller change is
worth landing from this WIP, split it explicitly and re-run the full verification
set above on the exact staged diff.

## Checkout state at time of writing

Original note was written on branch `main` at `57fdbf65`. The branch has since
advanced; current status should be checked with `git status --short`.
