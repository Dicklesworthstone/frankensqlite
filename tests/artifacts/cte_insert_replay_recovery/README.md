# CTE INSERT replay: unapplied recovery checkpoint

**These are recovery patches, not active engine fixes. No bead is closed by this checkpoint.**

The execution environment became unavailable on 2026-11-13 before the edited working tree could be committed, rebased, formatted, or validated. The GitHub connector remained available but could not retrieve the 13 MB connection.rs. The engine changes below were reconstructed from retained patches and source reads; their application still needs verification in a working checkout. The regression insertion was recovered from the complete test-file contents and compared with the unchanged upstream original.

## Targeted failures

- bd-r5mmz (P0): CTE reads in UPSERT SET lose rows during trigger replay, allowing silent wrong results.
- bd-6hy0g: UPSERT and RETURNING subqueries read materialized CTE roots through the main pager.
- bd-n2ehh: a caller CTE shadows the persistent relation used by a trigger WHEN clause or body.
- bd-1ucvb: CTE INSERT SELECT replay keeps the materialization fallback override alive and repeatedly reloads untouched rows.

The planned changes preserve materialized roots through mirror refreshes, explicitly route them to MemDatabase, isolate trigger namespaces, separate a same-named writable target from its CTE read binding, and freeze the original parameter slots before per-row replay.

## Checkpoint completeness and application

The files use Codex apply_patch format with repository-relative paths. They are intentionally stored outside compiled source.

1. 01_roots_and_binding.apply.patch: storage routing, row preservation, original parameter binding.
2. 02_cte_scope_and_target.apply.patch: required visibility guard and lexical read rebinding. **Pending the next recovery commit; do not apply this checkpoint without it.**
3. 03_obsolete_root_probe_cleanup.apply.patch: remove superseded root-probing functions and update their comments.
4. 04_sqlite_oracles.apply.patch: eight targeted regression tests, including memory/file, prepared/ad hoc, trigger, parameter, nested-scope, repeated-execution, and rollback cases.

Apply all correctness patches before compiling; patch 01 references the guard defined by patch 02. Work began at f437189e13ffe3b37bff2160c4c770b894d3a280. Four unrelated upstream commits reached main at 716b6ddc258493da9da8aba885fda2561a219659 while work was in progress. Preserve those changes when applying and reviewing this reconstruction.

## Validation actually observed

- Starting-revision cargo fmt --check passed.
- The new regression files passed rustfmt checks and whitespace checks before the environment failure.
- SQLite 3.53.1 independently confirmed selected fixture expectations, including the unbound-parameter and nested-scope results.
- Workspace cargo check and an initial targeted cargo test were killed while compiling asupersync 0.6.2.
- An isolated baseline CTE test build timed out after 1,201 seconds in dependencies; no SQL tests executed.
- A current-tree core all-targets check received an external SIGKILL after 290 seconds, still in dependencies.
- The final formatter, direct-binary retry, and Clippy did not yield completed results before the executor became unavailable.

**There is no passing Rust compilation or SQL-test result for these changes. Reconstructed patch applicability and final formatting are unverified.**

## Resume work

Review and apply the patches in a working checkout, run cargo fmt --check and cargo check --workspace --all-targets, then run the bd_cte_composition_edges_oracle_probe and bd_qteu2_memdb_reload_row_rehydration_scaling integration targets. Finish with the relevant existing trigger/TEMP/RETURNING tests and cargo clippy --workspace --all-targets -- -D warnings. Keep concurrent mode and assertions enabled.

Two known boundaries still need separate attention: attached-schema INSERT delegation does not yet apply the target-visibility guard; a trigger mentioning an otherwise nonexistent relation exactly equal to a generated private identifier could accidentally see a hidden binding. Neither boundary has been claimed fixed.
