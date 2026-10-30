# GH#530: Streaming implementation and partial qualification, 2026-10-30

The preserved candidate is now incorporated into production engine code in
[`b56e7bc7c93d6a04cfefb30cae82ec0a2fe7fcad`](https://github.com/Dicklesworthstone/frankensqlite/commit/b56e7bc7c93d6a04cfefb30cae82ec0a2fe7fcad),
with additional compatibility fixes and regression assertions. It was applied to
main baseline `f5cf83b283f0655e9b98dac7170a7f3d6b404fbd` from the retained
[candidate.patch](../../tests/artifacts/perf/issue-530-streaming-candidate/candidate.patch),
rather than recreated. The original patch remains unchanged as historical evidence.

The final engine, CLI, and six relevant integration targets pass Rust compiler
typechecking. Four prerequisite unit suites executed successfully: **335 passed,
two existing tests ignored**. However, native executable generation remains
blocked by the worker's combined memory and storage limits. **The connection-level
SQL integration assertions did not execute, no baseline or candidate CLI was
produced, and there is no measured before-and-after speedup or query-memory
reduction.** Issue #530 therefore still needs executable and performance
qualification; this report does not claim that those requirements are complete.

The [machine-readable summary](../../tests/artifacts/perf/issue-530-streaming-20261030/validation-summary.json),
[source hashes](../../tests/artifacts/perf/issue-530-streaming-20261030/source-sha256.json),
[evidence manifest](../../tests/artifacts/perf/issue-530-streaming-20261030/evidence-manifest.json),
and [raw-log archive](../../tests/artifacts/perf/issue-530-streaming-20261030/qualification-logs.tar.gz)
preserve the distinction between completed checks and remaining requirements.

## What the engine changes

For admitted independent Cartesian products of the unchanged built-in
`generate_series`, `json_each`, and `json_tree` modules, the executor scans each
source in FROM order, retains its input rows, and enumerates the product into one
reusable combined row. Each tuple is folded directly into the aggregate state.
It no longer needs to materialize all joined tuples before aggregating this
query class. There is no series formula or multiplication of input cardinalities
in place of SQL tuple evaluation.

The retained input storage is proportional to the sum of the input rows, plus
aggregate/group state and result storage. This is a property of the implementation,
not an executed memory measurement. A GROUP BY that creates one group per product
tuple can still require memory proportional to the product. Tuple enumeration
still performs work proportional to the Cartesian cardinality.

Borrowed-row aggregation shares the existing grouping machinery. Streaming SUM
and AVG now use the canonical `SumFunc` and `AvgFunc` states instead of separate
accumulators with different precision, coercion, NULL, and overflow behavior.
Product enumeration checks the operation context every 256 tuples so admitted
products observe cancellation and deadlines after source scanning finishes.

### Compatibility corrections beyond the retained draft

| Concern | Implementation and regression coverage |
|---|---|
| Custom modules and functions | Admission verifies live factory identity, declared layout, built-in scalar/aggregate identity, and the built-in BINARY collation. Replacements and unsupported callbacks retain the established executor. Tests include replacements at later source positions and callbacks that change registrations. |
| Hidden columns | Virtual-table metadata now declares hidden column names through a default trait method. Full binding layouts retain hidden columns; wildcard expansion and NATURAL matching use visible columns. Explicit references and USING of hidden columns remain possible. Tests cover custom names, replaced built-in names, aliases, derived tables, CTEs, and chained USING/NATURAL joins. |
| JSON cursor metadata | `json_each` and `json_tree` expose their hidden `json` and `root` arguments, typeless declared affinity, and zero-based scan rowids. Cursors preserve argument storage representations, clear state before refiltering, and enforce arity. The three new native JSON tests executed. SQLite-private `id` and `parent` traversal numbering is not asserted equal. |
| Empty products and errors | Static arity and column validation occur before execution. Once an earlier source is empty, later source arguments and scans are skipped, avoiding otherwise-unused errors. The draft's scan-every-source behavior was corrected. |
| Argument evaluation | Built-in `coalesce` and `ifnull` evaluate only the needed arms in the join expression evaluator. Application replacements retain their existing dispatch. Unsupported argument forms, lateral references, and application function behavior fall back. |
| NULL, precision, and overflow | Assertions cover empty and NULL inputs, typed JSON values, integer prefixes beyond 2^53, mixed REAL/integer inputs, infinities, and overflow/error recovery. Canonical aggregate units executed; connection-level assertions only typechecked. |
| Error order and representatives | Grouped product aggregate arguments containing scalar calls retain the existing path, preserving sorted-group error precedence. Projected grouping keys with MIN/MAX also fall back because equal keys can have different storage classes and require the extremum's representative row. |
| Reentrant metadata | Hidden-star expansion retries from the original unexpanded AST with the existing four-attempt bound. Module-only changes rebuild the entire projection; scalar changes preserve the established deferred/frozen VALUES-donor rules. Tests cover transient replacement, persistent churn, recovery, and direct/prepared statements. |

The custom argument callback-frequency assertion deliberately preserves
FrankenSQLite's existing fallback behavior. It does not claim that stock SQLite
invokes a custom callback equally often. SQL error and result assertions are
separate from fast-path admission and performance evidence.

### Deliberate admission limits

The product optimization excludes predicates, HAVING, lateral sources, custom
modules, outer joins, NATURAL joins, constrained ON/USING joins, application
functions or aggregates, replacement/non-BINARY comparison semantics, DISTINCT,
aggregate FILTER/ORDER BY and other unsupported aggregate forms, windows,
compound SELECTs, WITH, and attached ORDER BY or LIMIT/OFFSET. These queries keep
the established execution path. The HAVING, filtered, and ordinary-table benchmark
cases remain useful controls. Grouped expressions are admitted only where the
existing evaluator and representative-row behavior are supported; the exclusions
above are semantic safeguards, not evidence that the general path is faster.

## Executed validation

The pinned repository toolchain was installed and used:
`nightly-2026-08-31`, rustc `1.100.0-nightly (908501772 2026-08-30)`.
Default features and locked dependencies were retained. Development-profile
settings were one Cargo job, no incremental compilation, debug information off,
256 codegen units, LTO off, and `RUSTFLAGS='-Zthreads=1 -Cembed-bitcode=no'`.
The workspace dev optimization level is 1, with its existing `asupersync`
override at 0. These settings do not represent a release-profile benchmark.

| Check | Executed outcome | Archive evidence |
|---|---|---|
| Final engine, CLI, six integration targets | `cargo check` passed; final seven source hashes unchanged across the check | `native-final-logs/final-formatted-integration-check.*`, `formatted-source-{before,after}.json` |
| JSON unit suite | 228 passed, zero failed, one existing ignored | `logs/candidate-json-unit-tests-fixed.*` |
| Canonical aggregate units | 69 passed, zero failed, one existing ignored | `logs/candidate-aggregate-unit-tests-resumed.*` |
| Virtual-table units | 18 passed, zero failed | `logs/candidate-vtab-unit-tests-resumed.*` |
| `generate_series` units | 20 passed, zero failed | `native-final-logs/candidate-series-unit-tests.*` |
| Benchmark protocol suite | 14 passed, including actual child-process protocol checks | `oracle/benchmark-protocol-tests.log` |
| Six small changed Rust files | Direct rustfmt check passed | `native-final-logs/final-small-file-fmt-fixed.*` |
| Source whitespace | `git diff --check` passed | `native-final-logs/final-diff-check.*` |
| Configured workspace formatting | Failed with the same normalized output as pristine baseline: 446 diff hunks each | `logs/baseline-workspace-fmt-check.log`, `native-final-logs/final-workspace-fmt.log`; [comparison](../../tests/artifacts/perf/issue-530-streaming-20261030/format-comparison.json) |
| Focused Clippy | Failed on existing `fsqlite-error/src/lib.rs:713` `decimal_bitwise_operands` lint | `native-final-logs/final-named-targets-clippy.*` |
| Workspace Clippy | Stopped by memory guard while checking `asupersync`; no pass | `native-final-logs/final-workspace-clippy-all-targets.*` |
| Workspace check, all targets | Interrupted; incomplete, with no recorded compiler defect | `logs/final-workspace-check-all-targets.*` |
| Core SQL integration execution | Not completed | Native build attempts below |
| Baseline/candidate timing and query RSS | Not performed; neither executable exists | `native-final-logs/final-resource-state.json` |

The final typecheck covered `generate_series_constraints`,
`join_aggregate_lookup_oracle`, `gh436_streaming_function_calls`,
`json_tvf_oracle`, `table_function_subquery_argument_oracle`, and
`gh438_insert_select_table_function`. The two changed core targets now contain
28 test functions, versus eight at baseline: the preserved ten additions plus
ten further regressions. This count is a source inventory, not 28 passing SQL
tests. The three other JSON additions are included in the executed 228-test
JSON unit suite.

The final cached check succeeded after five formatting-only corrections in the
late callback tests. The earlier noncached check also accepted the final engine
and callback-retry semantics. The repository's configured formatter excludes
the very large core source; the six-file direct check does not claim whole-core
formatting acceptance. No dependency source, features, assertions, or lint
allowances were weakened to obtain a passing result.

Stock SQLite 3.53.2 was built from the official amalgamation ZIP, whose SHA-256
matches the historical investigation, and selected regression SQL was exercised
against that native oracle. Its source ID is
`d6e03d8c777cfa2d35e3b60d8ec3e0187f3e9f99d8e2ee9cac695fd6fcdf1a24`,
also matching the bundled rusqlite oracle. The archive retains the C build command,
the stock-only SQL results and errors, and the binary identity in the summary.
These oracle executions do not substitute for candidate execution.

## Native build blockers and measurement limits

The worker has an 8 GiB cgroup memory limit. Native attempts used the pinned
compiler and unchanged dependency/feature graph. The principal attempts were:

| Native attempt | Observed outcome |
|---|---|
| Ordinary combined CLI and integration build | Stopped inside `asupersync` after 152.13 s; sampled process-group RSS reached 7,191.46 MiB, above the 7 GiB guard. |
| Serial backend for `asupersync` | Stopped after 110.08 s when shared disk space fell below the guard; sampled process-group RSS reached 6,599.27 MiB. |
| Serial backend plus MIR memory settings | Stopped after 88.05 s on shared anonymous-memory pressure; sampled process-group RSS reached about 5.53 GiB before `asupersync` finished typechecking. |

These are **compiler-process resource measurements**, not query-memory results.
Raw samples, commands, environment values, time-pass diagnostics, guard reasons,
and the resource-monitor scripts are retained in the archive. Earlier rejected
option spellings are retained too: the pinned compiler removed
`-Zno-parallel-backend`; subsequent attempts used
`-Zunstable-options --jobs-backend=1`. A rejected option is a setup failure,
not a compiler-memory result.

At the final snapshot, the root filesystem had zero unreserved space and only
16 MiB reported free including reserved blocks. Creating the evidence directory
subsequently failed with ENOSPC. To preserve the work without deleting files,
the task's build target and toolchain had been moved to RAM-backed storage;
the cgroup then contained about 2.24 GiB of shared memory, including those relocated
files. Shared capacity fluctuated during the work; this is a timestamped resource
snapshot, not a claim that the filesystem will remain full. There was no additional
disk filesystem or usable remote compiler, and existing unrelated native caches
had incompatible compiler/dependency fingerprints and lacked `test-internals`.
They were not substituted. Only this task's jobs and files were managed.

This combination prevented a credible further identical native build. The
preserved logs distinguish safe guard stops, manual interruption, option errors,
compiler errors, and successful checks. The small final evidence archive was
staged separately for direct GitHub upload after repository file creation became
impossible.

The earlier [release/stock investigation](issue-530-cartesian-aggregate-investigation.md)
measured v0.4.12 Cartesian COUNT at 1.7065 s versus stock at 0.014578 s, and SUM
at 1.9785 s versus 0.0468235 s. Those remain historical release/stock results.
They must not be presented as the baseline of an executed comparison with this
implementation. The earlier RSS experiments likewise do not measure this code.

## Remaining qualification and reproduction

On a worker with sufficient memory and disk, first execute the two changed core
targets and the four nearby targets above. Also run the existing core unit
`test_values_donor_freezes_per_registry_snapshot_and_retries_reentrant_validation`,
which exercises the compatibility contract used by the metadata retry fix, and
complete the repository workspace gates. Typechecking has not established that
these runtime assertions pass.

For a development-profile comparison matching this qualification setup, keep the
same compiler, flags, profile, and feature selection for both revisions. The
following uses one shared build cache but copies each executable before the next
revision overwrites the build output. Choose a working directory with ample disk
space; these commands are follow-up instructions, not executed benchmark claims.

```bash
issue530_work=/absolute/path/with/ample-space/issue530
mkdir -p "$issue530_work"
git worktree add --detach "$issue530_work/baseline" \
  f5cf83b283f0655e9b98dac7170a7f3d6b404fbd
git worktree add --detach "$issue530_work/candidate" \
  b56e7bc7c93d6a04cfefb30cae82ec0a2fe7fcad
export CARGO_TARGET_DIR="$issue530_work/target"
export CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_DEV_CODEGEN_UNITS=256
export CARGO_PROFILE_DEV_LTO=false MALLOC_ARENA_MAX=1
export RUSTFLAGS='-Zthreads=1 -Cembed-bitcode=no'
for issue530_variant in baseline candidate; do
  cargo +nightly-2026-08-31 build --locked \
    --manifest-path "$issue530_work/$issue530_variant/Cargo.toml" \
    -p fsqlite-cli -p fsqlite-core --bin fsqlite \
    --test generate_series_constraints --test join_aggregate_lookup_oracle \
    --test gh436_streaming_function_calls --test json_tvf_oracle \
    --test table_function_subquery_argument_oracle \
    --test gh438_insert_select_table_function
  cp "$CARGO_TARGET_DIR/debug/fsqlite" "$issue530_work/$issue530_variant-fsqlite"
done
cargo +nightly-2026-08-31 test --locked \
  --manifest-path "$issue530_work/candidate/Cargo.toml" -p fsqlite-core \
  --test generate_series_constraints --test join_aggregate_lookup_oracle \
  --test gh436_streaming_function_calls --test json_tvf_oracle \
  --test table_function_subquery_argument_oracle \
  --test gh438_insert_select_table_function
cargo +nightly-2026-08-31 test --locked \
  --manifest-path "$issue530_work/candidate/Cargo.toml" -p fsqlite-core --lib \
  test_values_donor_freezes_per_registry_snapshot_and_retries_reentrant_validation
cargo +nightly-2026-08-31 check --locked --workspace --all-targets \
  --manifest-path "$issue530_work/candidate/Cargo.toml"
cargo +nightly-2026-08-31 clippy --locked --workspace --all-targets \
  --manifest-path "$issue530_work/candidate/Cargo.toml" -- -D warnings
cargo +nightly-2026-08-31 fmt --manifest-path "$issue530_work/candidate/Cargo.toml" --check
```

If the compiler-memory settings from a later attempt are necessary, apply them
identically to both native builds and record them in `--build-note`. The archived
JSON files preserve their exact arguments. Do not compare executables built with
different profile or feature settings. For release-profile claims, rebuild both
revisions in the same release profile and identify the resulting binaries.

Use the existing benchmark, with live stock SQLite 3.53.2 built as documented in
the historical investigation. Stop builds and profiling before collecting timing.
Omitting `--case` selects all sixteen existing cases: the original four queries,
constant/left/right SUM controls, asymmetric products in both orders, a three-way
product, HAVING and WHERE fallbacks, ordinary tables, and JSON products.

```bash
python3 scripts/benchmark_issue_530.py \
  --baseline-fsqlite "$issue530_work/baseline-fsqlite" \
  --baseline-revision f5cf83b283f0655e9b98dac7170a7f3d6b404fbd \
  --fsqlite "$issue530_work/candidate-fsqlite" \
  --candidate-revision b56e7bc7c93d6a04cfefb30cae82ec0a2fe7fcad \
  --sqlite /absolute/path/to/sqlite3 \
  --stock-revision d6e03d8c777cfa2d35e3b60d8ec3e0187f3e9f99d8e2ee9cac695fd6fcdf1a24 \
  --build-note 'Both Rust revisions: nightly-2026-08-31, dev opt1/asupersync opt0, debug0, cgu256, LTOoff, -Zthreads=1 -Cembed-bitcode=no; record exact stock compiler/flags' \
  --repeats 6 --warmups 1 \
  --work-dir "$issue530_work/timing-logs" --output "$issue530_work/timing.json"
python3 scripts/benchmark_issue_530.py \
  --baseline-fsqlite "$issue530_work/baseline-fsqlite" \
  --baseline-revision f5cf83b283f0655e9b98dac7170a7f3d6b404fbd \
  --fsqlite "$issue530_work/candidate-fsqlite" \
  --candidate-revision b56e7bc7c93d6a04cfefb30cae82ec0a2fe7fcad \
  --sqlite /absolute/path/to/sqlite3 \
  --stock-revision d6e03d8c777cfa2d35e3b60d8ec3e0187f3e9f99d8e2ee9cac695fd6fcdf1a24 \
  --build-note 'Same binaries as timing; separate Linux /proc RSS experiment' \
  --memory --memory-queries 3 --repeats 3 --warmups 0 \
  --work-dir "$issue530_work/memory-logs" --output "$issue530_work/memory.json"
```

The harness validates every warmup and measured result, rotates all six
three-engine orders over six timing repetitions, and records binary hashes,
raw native timers, plans, and failures. CLI timing includes the statement and
scalar output; shell preparation boundaries can differ, and FrankenSQLite's wall
timer rounds to milliseconds. The sixteen-case matrix currently has no grouped
performance case; grouped correctness assertions must still execute separately.

Memory mode is a separate run. Its first-query VmHWM includes process startup and
fixture setup, while later VmRSS checkpoints measure process memory retained after
completed queries. Neither counter isolates query heap nor proves a leak. The
collector requires `/proc` to expose the same PID namespace as its children; this
worker's successful protocol checks used matching namespaces. Preserve all raw
samples and report worker contention and measurement scope with any future result.
