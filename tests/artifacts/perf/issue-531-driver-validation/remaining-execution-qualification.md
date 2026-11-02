# Issue #531: applied planner and binding changes, remaining qualification

## Applied correction: bound ORDER BY values keep their expression meaning

The parameter binder now records whether each original ORDER BY expression is syntactically an ordinal before substituting parameters. If binding turns a nonordinal expression into an ordinal-looking integer, it retains the complete bound expression inside the existing `0 + expression` form. This covers unary signs, nested COLLATE wrappers, and absorbing constant folds while preserving genuine literal ordinals and their invalid-range errors.

The change is limited to `bind_placeholders_in_select_statement`. It does not apply the archived deferred projection or ordered join candidates. The existing scalar-subquery ORDER BY oracle test file now includes prepared-statement rebinding, ad hoc execution, seven wrapper forms, zero/negative/large bound values, genuine literal ordinals, and invalid literal ordinals under LIMIT 0. That public core regression has been reviewed and formatted but has not executed here.

### Executed binder qualification

The actual extracted Rust binder functions, complete parameter state, literal conversion and ordinal helpers were compiled and executed against the real locked AST, parser, types, error and function crates. No production function was mocked. Original parameterized SQL and the actual binder-serialized SQL were then compared in **C SQLite 3.53.2**.

| Result | Existing binder | Corrected binder |
| --- | ---: | ---: |
| Oracle cases | 95 | 95 |
| Original-versus-bound SQL mismatches | 23 | 0 |
| Preserve original top-level ordinal classification | Fails for affected wrappers | All tested cases pass |

Coverage includes mixed storage classes, unary signs and nested collations, anonymous/numbered/sparse/repeated named bindings, nested scalar SELECT and CTE ordering, LIMIT/OFFSET edge cases, and invalid literal ordinal errors. This is evidence for the Rust AST transformation and C SQLite semantics in those cases. It is not full native FrankenSQLite Connection/VDBE execution or a speed measurement.

- [Reproducible binder harness, source/dependency manifests and complete oracle receipts](binder-order-qualification-20261102.tar.gz)
- Archive SHA-256: `f7f1fa2f6113e10beb3dfc39cf2f2eae35083a740b9900805fc110fcf67939dc`.
- Applied connection.rs SHA-256: `e0293ffe696d6650d120ead3ad4f357deac474143809d4cfcaece9370ad7b941`.
- Applied scalar-subquery oracle test SHA-256: `b96e224f5e1e4a16ed91bf977c4686e284566aa9e75339ecf7e0da3e8b25bc33`.
- The applied binder function is identical to the qualified source except for the recorded rustfmt-only brace placement. Both the final function and new regression pass focused formatting checks.

The archived deferred-projection patch below predates this correction and must be reconciled with the applied source before further qualification. Full native core and realistic performance validation remain open.


## Current status: grouped planner improvement applied

The grouped equality-prefix change is now applied to production code, together with its bytecode regressions and core differential/work-budget extensions. The ordered-join and deferred-scalar-projection changes remain in the archived candidate patch below.

The production change selects an existing index equality prefix even when residual WHERE predicates remain. It rechecks the complete predicate on matching table rows, tracks distinct consumed conjuncts, and permits covering COUNT(*) input only when the full predicate is covered. It adds no opcode, storage behavior, transaction path, or aggregate accumulator.

### Verification of the applied grouped change

- Exact isolated production codegen SHA-256: `7a6d204483fc1f1d2aee0d8bb67a54726d6ec8d7a36414a04f580f9d1e714bfc`.
- **Full native VDBE crate metadata compilation through clippy-driver passed**, using the pinned compiler, unchanged locked native dependency graph, the crate's actual lint settings, and `--deny warnings`. Exit 0; no diagnostics; 11.333 seconds; recorded peak child RSS 765,440 KiB. The 7,261,879-byte metadata output was completed.
- **All fourteen grouped compiler/bytecode assertions passed on the preserved combined source.** Read-only comparison confirms that the grouped emitter in the applied subset is byte-identical to that tested source; its new selector differs only in a rustfmt whitespace adjustment. Four grouped assertions fail on baseline c5d0454.
- A fresh executable build of the isolated compiler harness reached archive creation and failed with ENOSPC. It did not execute another fourteen-test run.
- The new selector block and both changed test files pass focused rustfmt checks. Whole-file formatting aborted on both baseline and isolated codegen while attempting approximately 14 GB allocations. No workspace formatting pass is claimed.
- The core differential and 20,000-row work-budget tests are included but remain unexecuted. Full native core checks were killed; no SQL-result, transaction, runtime-work, or performance pass is claimed.
- Full workspace/all-target check and Clippy gates remain incomplete. The successful Clippy result is specifically the full native VDBE library target.

Changed test identities:

| File | SHA-256 |
| --- | --- |
| issue_531_grouped_index_input.rs | b9bd7d2dfb0964672d693f9fe4a97323d0b4c8b3b0fbf77f5bdc95d6b0c113a0 |
| bd_group_by_depth_oracle_probe.rs | 8f284a98e91af7a78d543de367c1e9d74e5580df15c74ba520098a2aef19bb8a |

Residual selectivity remains an unmeasured performance risk: probing a prefix matching nearly the entire table may cost more than a sequential scan. No statistics-based cost choice is added here. This is a planner improvement with compiler evidence, not a measured resolution of #531.

### Remaining connection work

Fresh review found a pre-existing binder defect inherited by the archived deferred-projection candidate: bound parameters wrapped in unary signs or COLLATE can be mistaken for ORDER BY output ordinals. An independently tested correction preserves original syntactic ordinal status. Ninety-five actual Rust binder transformations were compared with C SQLite 3.53.2: the existing binder had 23 mismatches and the correction had none. This proves the transformation for those cases, not native core execution. The deferred-projection candidate must incorporate that correction before application.

The final combined VDBE source also passed a native metadata check during this follow-up (source b2fc70..., exit 0, no diagnostics), but the core retry was killed again. No end-to-end speedup has been measured, and issue #531 remains open.

The patch below is still based on c5d0454 and includes the grouped changes now applied. Do not apply it wholesale to the updated main branch.

---

## Historical combined-candidate record at b689d276


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
