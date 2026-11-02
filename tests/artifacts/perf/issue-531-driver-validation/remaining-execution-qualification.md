# Issue #531: remaining execution candidate and qualification

Date: 2026-11-02 UTC  
Baseline: `c5d04545807faf2d135e2dd561e76aa16d5bb95d`  
Status: **candidate preserved; production changes not applied; native qualification incomplete**

## What this commit establishes

This commit preserves the complete candidate patch, source manifests, compiler-test receipts, and failed native-build evidence for the remaining #531 mechanisms. It does not install the candidate in the engine and does not establish a SQL-correctness or performance win. Issue #531 remains unresolved.

The candidate applies cleanly to the baseline above. The patch contains seven regular source/test files (1,578 insertions, 39 deletions). It contains no workspace symlinks, preservation copies, generated binaries, dependency substitutions, or changes to benchmark acceptance gates.

- [Complete candidate patch](remaining-execution-candidate.patch)
- [Verification archive](remaining-execution-verification.tar.gz)
- [Reported workloads](https://github.com/Dicklesworthstone/frankensqlite/issues/531)
- [Earlier generic-join materialization investigation](https://github.com/Dicklesworthstone/frankensqlite/issues/527)

## Findings against the inspected baseline

The inspected main revision already contains indexed input for the simple equality and twenty-value IN grouped-count shapes. The focused compiler tests for those shapes pass on the baseline. This is source/bytecode evidence; it does not establish the baseline's end-to-end performance on the reporter's database.

Three remaining mechanisms motivated this candidate:

1. **Grouped equality prefixes with residual predicates.** The existing grouped input selection can lose an otherwise useful index when additional WHERE predicates remain. The candidate describes the usable equality prefix separately from the consumed conjuncts, rechecks the complete residual predicate on candidate table rows, and permits a covering COUNT(*) when the complete composite equality is covered. Distinct conjunct identities prevent repeated index columns and contradictory equalities from hiding a residual filter.
2. **Ordered lookup joins with a final LIMIT.** The existing lookup join can still lose an available outer index order and enter sorting or generic execution. The candidate proves the complete outer order, traverses the outer ordering index or rowid in the required direction, and retains the existing inner lookup. LIMIT/OFFSET counts rows after ON/WHERE and LEFT JOIN null extension. It does not cap outer rows before join multiplication or filtering.
3. **Result-list correlated scalar subqueries.** The generic correlated projection path can do work before the ordered, filtered, limited input is reduced. The candidate first executes a raw source projection with the original FROM/ON/WHERE/ORDER/LIMIT/OFFSET and then evaluates the original result expressions for the selected rows using the existing correlated evaluator. The reporter's aggregate scalar count is outside the existing scalar VDBE eligibility and can reach this path.

These are general planner/execution changes. No SQL text matching, fixture values, database-size thresholds, or reporter-specific table names are used to select them.

## Semantic boundaries and review

The grouped changes preserve index-hint, affinity, and collation guards. Raw compiler entry points conservatively decline multi-term anonymous/named placeholder rewrites that could change binding order; the public connection canonicalizes parameter slots before compilation.

The ordered join proof resolves result aliases, ordinals, and star expansion once. It requires rowid uniqueness or a complete non-null unique key at the relevant ordering boundary. Unproven ordering retains the existing fallback. Existing selective equality seeks, including selective swapped numeric/typeless inner equality probes, keep precedence.

The deferred scalar projection binds the entire original AST once before rewriting the raw projection. It validates the original SELECT even for LIMIT 0. It declines DISTINCT, grouping, windows, scalar-dependent ordering, scalar predicates in WHERE/ON, output-alias dependencies in WHERE/ON, non-plain sources, materialized/time-travel sources, live virtual tables, NATURAL/USING joins, and unsupported join kinds. Joined input must already satisfy the compiled join eligibility rules. It reuses the connection's existing subquery evaluator and transaction context.

Review identified and corrected two ordered-join issues before freezing the patch: a high auxiliary cursor number that would cause dense cursor allocation, and a plan choice that could displace an existing selective equality probe. The ordered cursor now uses the next low available slot.

One new compiler test initially expected a sorter for explicit JOIN COLLATE. The established compiler behavior for that shape is Unsupported, with connection-level fallback. The test was corrected to assert that existing behavior; production admission was not broadened and existing golden snapshots were not regenerated. The failed initial test run is retained in the archive.

## Verification actually completed

| Check | Result | What it proves |
| --- | --- | --- |
| Frozen candidate compiler/bytecode assertions | 18 passed, 0 failed | Exact final codegen plus unchanged production ProgramBuilder/VdbeProgram blocks compile with real locked leaf dependencies and emit the asserted plans |
| Shared assertions on baseline c5d0454 | 10 passed, 5 failed | The shared tests distinguish five candidate planning changes from baseline behavior |
| Full native VDBE metadata compilation | Passed on an earlier candidate snapshot | Native crate type checking for codegen SHA 956b6201..., before final ordered-join corrections |
| Full native core metadata compilation | SIGKILL, exit -9 | Incomplete; no successful core compilation claim |
| Patch whitespace check and clean application | Passed | Patch is structurally applicable to the stated baseline |
| Focused Rust formatting | Passed for small test files/new regions | Not a completed workspace formatting gate |
| Full native integration tests | Not compiled or executed | No result, binding, transaction, or storage-runtime proof |
| Required workspace check/clippy/fmt gates | Incomplete | Not qualified for production application |
| Prepared baseline/candidate/live-C-SQLite benchmark | Not executed | No speedup or remaining-gap measurements |

The 18 passing focused tests comprise fourteen grouped-input assertions and four ordered-join assertions. They do not execute the storage VM. The baseline failures cover residual equality index input, complete composite covering input, duplicate-equality residual handling, conservative raw-placeholder handling, and ordered outer-index traversal.

The successful native VDBE metadata check took 8.760 seconds with a recorded peak child RSS of 718,440 KiB. Its source SHA-256 was `956b6201608c822cc49249a34b789fa2f0df19c48418c51f36483aa566f120ec`; that is not the final candidate hash.

The full core metadata attempt terminated with SIGKILL after 81.628 seconds, with a recorded peak child RSS of 2,238,416 KiB. Its connection source hash was unchanged before and after the attempt. The reconstructed direct rustc invocation emitted check-cfg warnings; a replay should include `--check-cfg=cfg(test)` to match Cargo's expected configuration. Absence of source errors before termination is not a passing check.

An LLVM baseline executable build was interrupted during dependency compilation as storage filled. A Cranelift baseline build was killed compiling unchanged Asupersync. No native FrankenSQLite executable was produced for the requested comparison. A final native VDBE refresh could not even start because writing its command receipt failed with ENOSPC.

## Regression and benchmark coverage prepared, but not run

The patch extends the owning crates' existing grouped and join tests and adds `crates/fsqlite-core/tests/issue_531_query_performance.rs`. It includes:

- Prepared-statement rebinding and anonymous, named, reordered numbered, and mixed storage-class values.
- Residual predicates, composite indexes, index hints, collations, duplicate equalities, and disjunction controls.
- Inner and LEFT lookup joins, missing matches, duplicate matches, ascending/descending ordering, aliases, ordinals, LIMIT/OFFSET, and selective probe precedence.
- Correlated scalar counts, scalar-dependent filtering/sorting controls, invalid names/functions/scalar widths at LIMIT 0, writes inside a transaction, and rollback.
- Deterministic work checks on 20,000 rows with forced-scan controls.
- An ignored prepared benchmark with a default of 500,000 posts, a live bundled C SQLite comparator, identical initial database files, discarded warmups, five trials with alternating engine order, complete result materialization inside each timed query, typed result comparison outside timing, and final integrity checks.

Result comparison records SQLite storage classes, including integer versus real and floating-point bits. Ordinary SqliteValue equality would otherwise accept some storage-class mismatches.

The published CLI seed attempts two million likes inserts but creates **500,000 distinct likes** because of its key pattern and INSERT OR IGNORE. The benchmark reports the actual count. A four-likes-per-post fixture is a distinct synthetic dataset and must be labeled separately.

The benchmark prints the actual bundled SQLite version. The previously built standalone SQLite 3.53.2 shell is not evidence that an unexecuted rusqlite benchmark used that version; verify the runtime version in the future native run.

## Frozen source and artifact identities

| Item | SHA-256 |
| --- | --- |
| Candidate patch | cd1bcaec3b386fab0230c80f8b698976ab9e6781bc7f84b491fbd8a570a81ff9 |
| Verification archive | 56d5c15080af6ba1347201de55890a505f5c758fb515a9b20946ae8029cb8805 |
| Final codegen.rs | b2fc70cc7754d4cb6db3d4952b467e474672ab9312d7cbda5b1635c05f9598fc |
| Final connection.rs | f6e6fabf7cf5b322cac3c3367996cb9b9baf2ce1de3c3adaeca3bf76e8124e59 |
| Final golden_bytecode_snapshots.rs | 215b52697e162d3f6d1c7ae74ac0eb822620c91ac82f1bb462af386ea16ac8d3 |
| Final issue_531_grouped_index_input.rs | b9bd7d2dfb0964672d693f9fe4a97323d0b4c8b3b0fbf77f5bdc95d6b0c113a0 |
| New issue_531_query_performance.rs | 7e5d496b7173d615cfbce83a9c7855994c423b554ae1391ba35d210c725745bd |

Patch size: 83,853 bytes; Git blob `6729322b259184d931b603a92e75621120964a0e`.  
Archive size: 566,740 bytes; Git blob `15a47e9d7377850487e1631f5d84aaf9c309c067`.

The archive contains the actual source captures, command arrays, source/dependency manifests, stdout/stderr, initial failed assertions, corrected 18/18 result, baseline 10/15 result, and native metadata-check receipts. Generated binaries and rlibs/rmeta files are excluded. Paths in the receipts describe the original worker and require relocation or fresh Cargo builds elsewhere.

## Required next qualification

Use the pinned nightly in rust-toolchain.toml, the unchanged lockfile and dependency graph, and equivalent build settings for baseline and candidate. Do not substitute incompatible dependency rlibs or remove runtime functionality to make the build fit.

Apply the patch in an isolated checkout based on the stated baseline, then run:

```bash
cargo test --locked -p fsqlite-vdbe \
  --test issue_531_grouped_index_input \
  --test golden_bytecode_snapshots \
  -- --test-threads=1

cargo test --locked -p fsqlite-core \
  --test issue_531_query_performance \
  --test bd_group_by_depth_oracle_probe \
  --test bd_9gl3w_join_limit \
  --test gh432_correlated_exists_index_probe \
  --test join_order_collation_oracle \
  --test correlated_subquery_oracle \
  --test join_lookup_affinity_oracle \
  -- --test-threads=1

cargo check --locked --workspace --all-targets
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo fmt --all --check
```

For the realistic prepared comparison:

```bash
FSQLITE_531_POSTS=500000 FSQLITE_531_TRIAL_MS=2000 \
cargo test --locked --profile release-perf -p fsqlite-core \
  --test issue_531_query_performance \
  -- --ignored --exact benchmark_prepared_issue_531 \
  --nocapture --test-threads=1
```

Run the same benchmark source on both the pristine baseline and candidate, with the live C comparator in each invocation. Record exact source, lockfile, toolchain, flags/features/profile, executable hashes, database cardinalities, SQLite version, and raw samples. Inspect remaining material gaps and run the related correctness controls before applying any production change to main.

The worker exhausted both its writable filesystem and shared-memory storage. Task-owned disposable compiler/cache directories were identified, totaling approximately 1.60 GiB, but were not removed: AGENTS.md Rule 1 expressly requires written permission even for newly created files. Source edits, baseline worktree, receipts, and database fixtures were preserved. Reclaiming those caches is a possible next step, not a guarantee that this worker has sufficient capacity for the complete native build.

**Acceptance remains open:** full-engine SQL equivalence, transactional behavior, parameter binding, realistic end-to-end improvement, and workspace gates must pass before this candidate can be described as a fix for #531.
