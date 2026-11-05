# GH#533: CTE investigation and benchmark qualification, 2026-11-05

The published v0.4.12 binary reproduces the issue's query-shape penalty on this
worker. **This is a release/stock investigation, not a current-main reproduction
or an executed before/after comparison.** The benchmark infrastructure is
verified; native compilation was blocked, so the proposed engine improvements remain
**UNVERIFIED and unapplied**. Correctness execution and paired measurements remain required.

The baseline source is `dbd27741cae17b596230a4a95b6f46dcc344fed9`. The verified
harness changes landed in
[`ffef921e803892d35acfa7f71a8860da8fef953b`](https://github.com/Dicklesworthstone/frankensqlite/commit/ffef921e803892d35acfa7f71a8860da8fef953b).
The [validation summary](../../tests/artifacts/perf/issue-533-20261105/validation-summary.json),
[manifest](../../tests/artifacts/perf/issue-533-20261105/evidence-manifest.json), and
[raw logs](../../tests/artifacts/perf/issue-533-20261105/raw-logs.tar.gz) preserve completed checks and failures.

## Prior work and semantic constraints

[Issue #533](https://github.com/Dicklesworthstone/frankensqlite/issues/533) reports
0.604/6.325 seconds for direct/CTE COUNT and 29.848/168.644 seconds for CTE/direct
correlated EXISTS. Those are contributor measurements, separate from the table below.
The [comprehensive specification](../planning/COMPREHENSIVE_SPEC_FOR_FRANKENSQLITE_V1.md)
requires SQLite CTE semantics; the [performance plan](../planning/PERFORMANCE_OPTIMIZATION_PLAN.md)
requires comparable, checked measurements. Existing
[`schema_only_cte.rs`](../../crates/fsqlite-core/src/connection/qualified_column_binding/schema_only_cte.rs)
admits persistent-table sources and excludes virtual tables: its presence does not
establish that `generate_series` CTEs avoid materialization.

The [#530 qualification report](issue-530-streaming-qualification.md) describes
historical implementation `b56e7bc7c93d6a04cfefb30cae82ec0a2fe7fcad`.
Commit [`ef756e77448aa4f3e0413d49b24496954d9d1d3d`](https://github.com/Dicklesworthstone/frankensqlite/commit/ef756e77448aa4f3e0413d49b24496954d9d1d3d)
backed out its production half on October 31 before 0.4.13: inconsistent hidden-column
star lists across joins, CTEs, views, CTAS, INSERT, and subqueries could misalign affinity
metadata and silently change filters. Tests and harness survived; #530 and
`bd-star-hidden-reland-nxl8a` remain open. This work does not restore that broad path.

[Relevant bead records](../../tests/artifacts/perf/issue-533-20261105/related-beads.json)
were read as constraints, not as evidence that every related query is already fast:

| Bead | Relevance |
|---|---|
| `bd-5ap77`, `bd-iuz8e` | Existing derived-table flattening and single-TVF streaming aggregates |
| `bd-wwqen.2`, `bd-k5isc` | Equality EXISTS memoization and remaining correlated CTE work |
| `bd-ztgst`, `bd-1dp9.6.7.4` | CTE-only hydration suppression and unfinished pager-backed cutover |
| `bd-gh-cte-materialization-hints-sn0yh`, `bd-wpiq6`, `bd-9tcne` | Hints, repeated volatile evaluation, dependency order, lexical shadowing |
| `bd-gh-cte-compiled-cache-stale-root-pbz9q` | Temporary root lifetime and compiled-cache bypass |
| `bd-czzlp`, `bd-star-hidden-reland-nxl8a` | Checked benchmark answers and hidden-column/affinity safety |

## Exact reported SQL and published-binary observations

```sql
SELECT count(*) FROM generate_series(1,1000000);
WITH t AS (SELECT value FROM generate_series(1,1000000)) SELECT count(*) FROM t;
WITH t AS (SELECT value FROM generate_series(1,10000))
SELECT count(*) FROM t WHERE EXISTS(SELECT 1 FROM t t1 WHERE t.value=t1.value);
SELECT count(*) FROM generate_series(1,10000)t
WHERE EXISTS(SELECT 1 FROM generate_series(1,10000)t1 WHERE t.value=t1.value);
```

[The complete timing receipt](../../tests/artifacts/perf/issue-533-20261105/release-stock-timing.json)
records one fresh-process observation per engine/case, zero warmups, default settings,
empty explicit init files, and native CLI SELECT timers. Startup and setup are excluded.
This was a shared Linux x86-64 worker with an 8 GiB memory limit and external build
contention; these observations are not stable throughput estimates. All four answers
matched: 1,000,000 for COUNT and 10,000 for correlated EXISTS.

| Query | v0.4.12 wall seconds | v0.4.12 CPU seconds | SQLite 3.54.1 wall seconds | SQLite CPU seconds |
|---|---:|---:|---:|---:|
| Direct COUNT | 0.339 | 0.122417 | 0.036059 | 0.015343 |
| CTE COUNT | 7.296 | 2.505219 | 0.030047 | 0.012474 |
| CTE correlated EXISTS | 16.196 | 8.119247 | 0.004853 | 0.001353 |
| Direct correlated EXISTS | 96.699 | 77.966582 | 0.000922 | 0.000928 |

The direction of both query-shape differences agrees with the report. These numbers
do not measure the candidate, and release-versus-stock ratios are not candidate gains.
[Native provenance](../../tests/artifacts/perf/issue-533-20261105/native-provenance.json)
records download URLs, full archive/binary SHA-256 hashes, sizes, and version IDs.
FrankenSQLite is the official portable v0.4.12 asset; stock is the official 3.54.1
Linux tools binary, source ID `555e07ef33f69a0957d24d8e555b702464a131d6f1607f4c3c1e4551e8e6ca76`.

### Plans, stages, and memory boundaries

Stock's two COUNT plans both scan `generate_series` directly. Its correlated plans
pass equality bounds to the virtual cursor and terminate after the first match.
The release's EXPLAIN QUERY PLAN returns only outer SCAN descriptions; full EXPLAIN
fails with `WITH clauses require connection-level CTE lowering or an explicit fallback
boundary` or `non-table FROM source`. Raw stdout, stderr, SQL, and return codes are
retained. The incomplete release plan is not a complete executed-stage profile.

Baseline source inspection identifies different routes in
[`connection.rs`](../../crates/fsqlite-core/src/connection.rs): direct COUNT can use the
single-TVF aggregate stream, whereas the CTE body first produces rows, copies them into
a temporary MemTable, then executes the consumer. General table-function subquery
execution can materialize the inner series before filtering; a correlated EXISTS can
repeat that work. The exact-equality MemDB memo has a global hydration gate, although
statement-owned CTE rows are already authoritative. These are source-derived causes
to test, not a measured breakdown of the wall-time table.

The separate [memory experiment](../../tests/artifacts/perf/issue-533-20261105/release-stock-memory.json)
completed all four cases, with one fresh process per engine/case and two checked queries.
The table uses **KiB**; HWM is the first-query high-water mark and RSS is sampled after
queries 1 and 2 in the same live child. No candidate memory reduction is established.

| Query | v0.4.12 first HWM | v0.4.12 RSS 1 → 2 | Stock first HWM | Stock RSS 1 → 2 |
|---|---:|---:|---:|---:|
| Direct COUNT | 12,864 | 12,864 → 12,864 | 2,588 | 2,588 → 2,588 |
| CTE COUNT | 588,036 | 104,796 → 206,260 | 2,628 | 2,628 → 2,628 |
| CTE correlated EXISTS | 19,460 | 7,744 → 8,776 | 2,488 | 2,488 → 2,488 |
| Direct correlated EXISTS | 19,972 | 5,200 → 8,640 | 2,624 | 2,624 → 2,624 |

These are whole-process resident counters, including startup/setup, allocator retention,
caches, and effects of shared-host memory pressure. CTE COUNT's second HWM was 684,804 KiB;
two query completions do not establish a leak. The smaller
[protocol example](../../tests/artifacts/perf/issue-533-20261105/memory-protocol-example.json)
validates the checkpoint mechanism separately. Both experiments finished before the
third source-build attempt described below started.

## Verified benchmark improvements and controls

`--issue 533` adds 21 cases while `--issue 530` retains the original 16-case suite.
Controls cover both materialization hints, body/consumer filters, projection and named
columns, hidden start/stop/step values, a conflicting hidden-step predicate, LIMIT/OFFSET,
ordered LIMIT, empty input, multiple references, correlated matches, misses, and offsets.
[All 21 stock assertions passed](../../tests/artifacts/perf/issue-533-20261105/stock-correctness-21.json).
Those are deterministic correctness results, not engine-candidate test results.

The unchanged baseline protocol suite ran 14 tests with **1 failure, 2 errors, 0 skips**;
the updated suite ran **20 tests, all passing, 0 skips**. Both logs are retained beside
the manifest. In this container, `Popen.pid` belongs to a different namespace from the
mounted `/proc`: baseline memory reads fail with `FileNotFoundError` for that PID.
A shell-builtin handshake now reports `/proc/self/stat`'s PID on a dedicated pipe before
`exec` replaces the shell with the native CLI. Local Popen PID still owns kill/wait;
only the validated handshake PID selects `/proc/.../status`. No PID scanning or Python
child wrapper is used. Malformed/missing handshakes, timeouts, high descriptor numbers,
wrong answers, stderr errors, missing markers/counters, and child cleanup are tested.

Zero wall timers now mean **below the CLI's resolution**; affected speedup ratios are
suppressed. FrankenSQLite prints wall time to milliseconds, so short valid controls
can print `0.000`. Negative/nonfinite/missing/extra timers and incorrect answers still
fail closed. This is a verified measurement fix; it supplies no engine speedup claim.
Paired candidate mode still requires baseline and candidate binaries, rotates all
three-engine orders, verifies every warmup/result, and separates memory from timing runs.

## Conservative engine design: UNVERIFIED

The [portable proposal](../../tests/artifacts/perf/issue-533-20261105/candidate-unverified.patch)
and [source/hash receipt](../../tests/artifacts/perf/issue-533-20261105/candidate-unverified.json)
preserve exactly four engine/test files against `931a0dd6b31d70d7c5d9fc66c01ce9df9e17b64e`.
Read-only forward application against the index and reverse application against the
worktree both passed. These checks establish applicability, not compiler/runtime success
or deployment. The base advanced after the original baseline snapshot; eventual
performance attribution must account for that drift or use matching base revisions.

The worktree candidate lowers one nonrecursive, single-use, unhinted CTE over the
unchanged built-in `generate_series` through existing derived-source handling. Only
bare-column projections with optional aliases are admitted; the derived flattener must
preserve visible/declared names and reject unexposed hidden columns. It declines scalar,
aggregate, module, and BINARY-collation overrides; clock literals and other volatile
expressions; computed CTE projections; nested/chained WITH; and inner ORDER/LIMIT barriers.
Materialization hints and multiple references retain their existing execution route.
Materialized row storage can also take ownership of `Row.values` instead of cloning it;
metadata is resolved before transfer and the mutable MemDB borrow ends before re-entry.

For direct correlated EXISTS, the candidate plans the unchanged built-in virtual cursor,
preserves hidden positional arguments and step alignment, validates xBestIndex argv
assignment, evaluates the complete residual predicate, and stops at the first match.
Custom factories/collations and volatile expressions retain the fallback. The CTE change
only admits authoritative materialized roots into the existing exact-equality memo;
TEXT-affinity operands decline. Generic/templated CTE scan admission is not broadened.
No closed-form count shortcut replaces SQL evaluation.

All three baseline native builds ended with **asupersync rustc SIGKILL before producing
a CLI**: default LLVM, asupersync/core Cranelift, and asupersync `hint-mostly-unused` with
core Cranelift. Commands, environment, and full logs are retained in the summary/archive.
A fourth attempt, candidate `cargo check --offline --locked -p fsqlite-core --lib`,
exited 101 with asupersync rustc SIGKILL before reaching fsqlite-core. No changed engine
code passed compiler or runtime qualification. This is a resource-blocked qualification,
not evidence of a candidate correctness failure or performance regression. Retry on a
worker able to compile the default native dependencies, use matching baseline/candidate
bases, execute the tests, then collect repeated paired timing and memory measurements.

The [resource snapshot](../../tests/artifacts/perf/issue-533-20261105/resource-limits.json)
at 22:10:57 UTC records an 8,589,934,592-byte limit and 8,589,795,328 bytes in use.
Cgroup `oom=159`/`oom_kill=26` counters are shared aggregates, not attribution of individual
compiler deaths. `cpu.max` is `800000 100000`; pinned rustc/cargo versions are recorded.
[Formatter receipts](../../tests/artifacts/perf/issue-533-20261105/formatter-receipts.json)
record workspace `cargo fmt --all -- --check` timing out after 120 seconds (exit 124),
with repository exclusions retained; no completed workspace formatting pass is claimed.
The explicit gh204-file `rustfmt --edition 2024 --check` with `ignore=[]` and
`skip_children=true` passed (exit 0). Formatting is source-only verification.
Clippy, Rust correctness execution, native stage execution, and paired candidate
measurements were **not completed**.

The [public-API stage probe](../../tests/artifacts/perf/issue-533-20261105/stage-probe/README.md)
is prepared but **UNEXECUTED**. Both offline metadata audits checked 215 registry packages
with zero version/checksum drift and no test/bench internals. These are dependency-resolution
checks, not native compilation or measured stage counters. Source, relative manifests,
locks, audit receipts, and portable preparation/build instructions are preserved.

### Existing compatibility gaps and lifetime controls

[Six tiny native affinity probes](../../tests/artifacts/perf/issue-533-20261105/cte-affinity-oracle.json)
found a pre-existing release discrepancy: a materialized `VALUES(1)` column compared
with a materialized `CAST(value AS TEXT)` column yields count 1 in stock and 0 in v0.4.12,
also with reversed operands or an added residual filter. Numeric/BLOB CAST controls
agree. Declining ambiguous CTE TEXT probes contains the candidate's admission surface;
it does not claim to fix the existing fallback's expression-affinity representation.

[Lifetime controls](../../tests/artifacts/perf/issue-533-20261105/cte-lifetime-oracle.json)
match stock/release at 6 for nested same-name CTE shadowing and 15 for repeated correlated
nested materialization. Source inspection finds monotonically allocated roots during
ordinary SELECT, exact-root cleanup, and memo clearing at the outermost WHERE guard.
A recursive inner self-probe is rejected by stock as multiple recursive references but
accepted by the release; this existing incompatibility is not a valid optimization control.
[Index-hint controls](../../tests/artifacts/perf/issue-533-20261105/cte-index-hint-oracle.json)
agree: `NOT INDEXED` returns 4, while `INDEXED BY missing_index` errors in both binaries.

To repeat the released-binary screen, use `scripts/benchmark_issue_530.py --issue 533`
with `--release-fsqlite`, `--release-revision v0.4.12`, `--sqlite`, the four reported
`--case` names, `--repeats 1 --warmups 0 --timeout 900`, and fresh `--work-dir`/`--output`
paths. Use `--memory --memory-queries 2` separately. A qualified optimization claim
requires replacing release mode with matched `--baseline-fsqlite` and `--fsqlite` builds
and completing deterministic engine tests before collecting repeated paired samples.
