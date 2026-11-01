# Issue #531: 2026-11-01 implementation checkpoint

## Status and provenance

This is a preservation checkpoint, not a qualified engine fix. The complete
implementation and proposed Rust regressions are in
[`unqualified-engine-candidate.patch`](unqualified-engine-candidate.patch).
The patch is not applied to production sources on main. No candidate engine
test, fresh baseline/candidate SQL comparison, query timing, or query RSS
measurement completed during this follow-up.

- Baseline: `770a812db0d494c46d900c5e5601768599d1bb34`.
- Patch SHA-256: `1772197dca2051309a656203cd9df4269fa8d1eaebc3cdd21677a0f309ca2808`.
- Unchanged Cargo.lock SHA-256:
  `828b244f97320a944275e708eb28be1032debad72acbb649f10dc03327cca502`.
- Pinned compiler: `rustc 1.100.0-nightly (908501772 2026-08-30)`, from the
  repository's `nightly-2026-08-31` toolchain.
- Scope: six engine/test files, 2,263 added lines and 85 removed lines.

The reporter's issue #531 measurements describe its tested revision, not this
baseline. The earlier benchmark-driver commit `09b996e8905fd88106bd5c22513898ee66c28243`
and candidate checkpoint `8767de5a9cb0bc612366daf1e9d58e2ad4ae9619`
were reviewed before this implementation follow-up. The previous candidate
remains recoverable through git history.

The investigation also read the comprehensive SQLite port specification,
`docs/planning/PERFORMANCE_OPTIMIZATION_PLAN.md`, the historical port plan,
the negative-results ledger, relevant `.beads/issues.jsonl` entries, and recent
join, aggregate, index-seek, and issue #527 materialization work. In particular,
`bd-oi8tv` documents broader missing correlated-subquery EQP output and is not
resolved by this scoped candidate.

## Source findings and intended execution

These findings are source-level routing analysis; they are not fresh profiling
of every query in the issue.

| SQL shape | Current source obstacle | Candidate path |
| --- | --- | --- |
| Selective equality/IN with GROUP BY and COUNT(*) | The grouped aggregate input path does not reuse selective equality/constant-IN index loops. | Seek only matching index prefixes and feed the existing grouping sorter; simple covered keys with plain COUNT(*) avoid table-row fetches entirely. |
| Lookup join ordered by the outer index with LIMIT | Limited-join admission excludes ORDER BY; native lookup generation does not preserve a proven outer index order. | Admit eligible ordered lookup joins and stream a proven rowid or index order, stopping at the result limit after correct residual filtering and OFFSET. |
| Indexed correlated COUNT(*) in an ordered projection | Native scalar admission and projection timing can prevent the indexed, limit-bounded route. | Validate the complete outer scope and builtin function identity, then reuse the scalar count seek kernel; evaluate the projection after streaming OFFSET. |
| Join plus correlated COUNT(*) | Composition can fail native admission even when each operation has an indexed implementation. | Validate direct COUNT(*) projections for admission, retain their original AST for execution, and pass all joined cursors into the existing scalar kernel. |
| EXPLAIN QUERY PLAN | Syntactic descriptions can hide actual join access or omit correlated work. | For supported unambiguous native shapes, describe the generated cursor operations and correlated parent/child access. |

The generic `execute_join_select` fallback is relevant to failed native join
admission. It does not explain the separate grouped aggregate code-generation
gap by itself. Issue #527's previous column-pruning work had already reduced
materialization; its remaining page-pool/memory concerns are separate from a
claim that every issue #531 slowdown has the same cause.

### Changes beyond the previous candidate

1. The grouped loop has its own `emit_index_prefix_rows` helper. The existing
   ungrouped aggregate seek emitter is unchanged. Repeated index columns do
   not accidentally prove that all WHERE predicates were enforced.
2. Covering GROUP BY is limited to direct stored INTEGER/TEXT grouping keys,
   plain COUNT(*), and a completely enforced single equality or residual-free
   constant IN predicate. Richer aggregates and residual predicates retain
   table reads and predicate evaluation.
3. Ordered join planning resolves aliases and ordinal positions, including
   expanded stars. A unique complete outer order is required before dropping
   the sorter. Selective outer seeks take priority over an ordered full scan.
   Other supported orders use the existing bounded top-N sorter.
4. Only callback-free stored-value outer predicates are pushed before the
   inner lookup. Independent ordering keys can reject a row from the bounded
   sorter before running its correlated projection. LIMIT 0 skips cleanup of
   cursors that were never opened.
5. Correlated-count admission excludes lazy OR/CASE/coalesce/ifnull/iif forms,
   row-value predicates, unsupported subquery structure, projected scalar
   aliases referenced in WHERE/ON/LIMIT, and ambiguous unqualified outer
   names. COUNT overrides and unsafe affinity/collation cases retain fallback.
   ORDER BY aliases and ordinal positions keep their intended meaning.
6. EQP recognition is conservative about self-joins, repeated root pages, and
   multiple competing cursor accesses. It does not advertise native execution
   for a fallback plan. Broader EQP gaps remain open.

The candidate uses ordinary statement cursors and existing transaction/index
interfaces. It does not change concurrent mode defaults, add global
serialization, or introduce a separate cache of correlated results. These
design properties do not substitute for runtime visibility/concurrency tests.

## Regression coverage added to the candidate

The three integration targets use live rusqlite oracles and engine work/plan
checks. Their new assertions remain unexecuted in FrankenSQLite:

- `bd_group_by_depth_oracle_probe`: equality and IN inputs, duplicate and NULL
  probes, affinity, composite/DESC indexes, NULL suffixes, residual predicates,
  repeated index columns, richer aggregate fallbacks, rebinding, rollback, and
  VM work against a forced scan. The narrow covering case requires no table
  rowid lookups.
- `bd_9gl3w_join_limit`: memory/file-backed ordered joins, LEFT JOIN null
  extension and multiplicity, mixed directions, NULL ordering, aliases and
  ordinals, partial-index decoys, NOT INDEXED, point-seek preference, count
  wrappers, prepared parameters, rollback/commit visibility, bounded sorters,
  LIMIT 0, and actual VM-opcode budgets on large outer inputs.
- `gh432_correlated_exists_index_probe`: joined and standalone count probes,
  prepared statements, COUNT replacement invalidation, affinity fallbacks,
  OFFSET/LIMIT-bounded callback counts, lazy expression error behavior, alias
  scopes, ambiguous outer columns, and all three relevant EQP families.

The work assertions count actual VM opcodes or callback invocations. They do
not purport to measure exact storage rows examined. The tests include controls
that must remain on the existing fallback path.

## Executed checks

| Check | Result | What it establishes |
| --- | --- | --- |
| Pinned rustfmt on all six changed files, edition 2024, `skip_children=true` | Passed | Focused formatting and parsing only. |
| `git diff --check` | Passed | No whitespace errors in the candidate. |
| Forward `git apply --check` in a clean baseline snapshot | Passed | Patch applies to the exact baseline. |
| Reverse `git apply --check` in the modified working tree | Passed | Patch represents the retained source edits. |
| `python3 -m unittest discover -s scripts/tests -p 'test_benchmark_issue_53[01].py' -v` | 30 passed in 1.482 seconds | Benchmark protocol validation only; 14 issue #530 tests and 16 issue #531 tests. |
| Narrow native VDBE cargo check | Failed in unchanged Asupersync 0.5.0 | Changed engine code was not compiled. |
| Native Rust SQL tests / workspace checks / Clippy / baseline-candidate benchmark | Not completed | No native acceptance or measured improvement. |

The first sandboxed Python test attempt had a `/proc` PID-namespace mismatch
in RSS collection. The same command passed when run with a matching process
namespace; no test or gate was changed. The passing output is
[`protocol-tests-20261101.log`](protocol-tests-20261101.log).

A separate in-memory C SQLite 3.53.1 check confirmed the new ambiguous-name
test's premise: a single-source bare `id` correlation returned `[(1, 2)]`,
whereas the joined version with two outer `id` columns failed with
`ambiguous column name: id`. This was an oracle-only check, not a comparison
against the candidate engine.

## Native validation blocker

The attempt used the pinned compiler and exact lockfile, with one job, no
incremental compilation, no debug information, and one compiler thread:

```sh
ulimit -v 1800000
export CARGO_HOME=/tmp/rch-cargo
export CARGO_TARGET_DIR=/tmp/rch-target-e6c5613756dc
export CARGO_BUILD_JOBS=1
export CARGO_PROFILE_DEV_DEBUG=0
export CARGO_PROFILE_TEST_DEBUG=0
export CARGO_INCREMENTAL=0
export RUSTFLAGS=-Zthreads=1
cargo check --locked --offline -p fsqlite-vdbe --no-default-features --lib
```

The compiler exited 101 after `memory allocation of 56885264 bytes failed`,
then Asupersync's rustc process aborted with SIGABRT. The complete failure is
[`native-check-20261101.log`](native-check-20261101.log). The virtual-memory
limit was protective, not a repository build requirement or proof that the
dependency cannot compile on a suitably provisioned worker.

The host has an 8 GiB cgroup memory limit with substantial existing shared
memory occupancy. Its 63 GiB shared disk has no unprivileged available space,
and its actual 4.9 GiB RAM filesystem is full. A sandbox view that appears to
show an empty RAM filesystem does not reflect that host capacity. A small
interrupted test edit was restored completely; the final six-file formatting
and patch checks above ran after restoration.

The earlier issue #530 qualification record was inspected for a reusable build
method. Its successful uncached check required 6,943,100,928 bytes peak
process-group RSS. Later smaller checks reused dependency artifacts; the
recorded cache paths are absent and that lockfile differs. Incompatible cached
artifacts were not substituted, dependencies were not changed, and no file was
deleted to create capacity. No alternate RCH runner was available.

## Qualification still required

Follow the commands in [README.md](README.md) on a sufficiently provisioned
worker. Run the three native integration targets, required workspace check,
Clippy, and workspace formatting gate, then build exact baseline and candidate
executables with identical compiler/profile/features/flags and lockfile.

Use `scripts/benchmark_issue_531.py` with live C SQLite in the same invocation.
Check every result, including duplicate multiplicity and ordered output, and
measure all six query families with both labelled full-scale fixture modes.
The supplied CLI generator makes two million insertion attempts but only
500,000 distinct likes; `four-per-post` creates two million distinct synthetic
likes. CLI timings include preparation and output and cannot be substituted
for the reporter's unattached prepared-API timings. Report query timing and
process RSS separately. Only then qualify production engine commits or claim
a performance improvement.
