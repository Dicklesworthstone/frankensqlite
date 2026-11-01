# Issue #531: benchmark validation and unqualified engine candidate

**2026-11-01 status: the engine candidate remains unapplied and has not passed
native compilation or SQL execution. No candidate speedup is established.**

The benchmark driver is committed and executable. Its protocol validation was
rerun on 2026-11-01: all 30 tests passed, including the 14 issue #530 tests.
The earlier included stock-only RSS smoke used C SQLite 3.53.2, 10 users,
100 posts, and 400 distinct likes under the explicitly labelled `four-per-post`
synthetic fixture. All 12 checked executions across six reported query families
matched that live oracle. These results validate the benchmark collector and
fixtures; they do not validate FrankenSQLite's candidate engine.

See [the dated qualification record](qualification-20261101.md) for source
findings, changes since the previous candidate, executed checks, raw logs, and
the remaining native qualification work.

## Preserved engine work

`unqualified-engine-candidate.patch` contains the complete candidate against
engine revision `770a812db0d494c46d900c5e5601768599d1bb34`. Forward applicability
to a clean snapshot of that revision and reverse applicability to the modified
working tree passed. This replaces the earlier candidate against `67478c6`.
It is an unapplied patch: this checkpoint does not change engine behavior on main.

The candidate reuses indexed equality/IN loops for grouped input, with a narrow
covering-index path for plain COUNT(*) and stored grouping keys. It preserves
outer ordered-index traversal through lookup joins, bounds eligible streams
with LIMIT/OFFSET, admits validated indexed correlated COUNT(*) projections,
and derives relevant EXPLAIN QUERY PLAN output from actual cursor operations.
The new grouped-loop helper leaves the existing ungrouped aggregate emitter
unchanged. Correlated-count admission excludes unsafe lazy expressions,
ambiguous outer names, and projected-alias dependencies in WHERE/ON/LIMIT.

The patch includes rusqlite-oracle tests for binding, NULL/collation/affinity
boundaries, join multiplicity, retained prepared statements, transaction
rollback, overridden COUNT functions, aliases, and LIMIT-bounded work. Grouped
work guards include a forced full-scan control and zero table-row lookups for
the covering path. These Rust tests have not executed.

Static review, focused formatting checks, diff checks, and patch applicability
are not substitutes for compiler or runtime acceptance. The latest narrowed
VDBE check stopped in unchanged Asupersync 0.5.0 at the protective compiler
memory limit, before compiling changed code. The raw failure is preserved.

The remaining optimization scope includes non-BINARY grouped predicates,
unfiltered ordered grouping, broader multi-source joins, and generalized
correlated expressions. Unsupported cases retain the existing execution path.

## Native qualification required before applying the engine changes to main

On a worker with sufficient disk and memory, apply the patch to a disposable
checkout of `770a812db0d494c46d900c5e5601768599d1bb34` or this checkpoint.
Use the repository's pinned nightly and unchanged dependency lock:

```sh
git apply --check tests/artifacts/perf/issue-531-driver-validation/unqualified-engine-candidate.patch
git apply tests/artifacts/perf/issue-531-driver-validation/unqualified-engine-candidate.patch
cargo test --locked -p fsqlite-core \
  --test bd_group_by_depth_oracle_probe \
  --test bd_9gl3w_join_limit \
  --test gh432_correlated_exists_index_probe
cargo check --locked --workspace --all-targets
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo fmt --all --check
```

Run `scripts/benchmark_issue_531.py --help` for matched baseline/candidate/stock
timing and separate Linux RSS experiments. The driver requires verified binary
identities and matching compiler/profile/features/flags/dependency locks.
Run both explicitly labelled full-scale fixture modes: the supplied CLI recipe
attempts two million likes but produces 500,000 distinct primary keys, while
`four-per-post` constructs two million distinct synthetic likes. Its CLI timing
includes parsing/preparation and result printing; it is not the reporter's
unattached prepared-API benchmark. Do not relabel process RSS, output rows, or
cursor-navigation opcode counts as exact rows examined.

## Current resource boundary

At this checkpoint the shared disk (63 GiB) had no unprivileged free space and
the actual host RAM filesystem (4.9 GiB) was full. The cgroup memory limit is
8 GiB, with substantial shared-memory occupancy from existing work. Available
cached Asupersync artifacts have incompatible dependency graphs and were not
substituted. The earlier issue #530 uncached successful check required about
6.94 GB peak process-group RSS; its cache paths are absent and its lockfile
differs. No source, cache, database, or evidence file was deleted to make room.
