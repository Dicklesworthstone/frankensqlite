# Issue #531: benchmark validation and unqualified engine candidate

**2026-11-02 status: the engine candidate remains unapplied and has not passed
native compilation or SQL execution. No candidate speedup is established.**

The [latest follow-up record](qualification-20261102.md) documents the reviewed
candidate against `bd569509d80427e11fb40d3654b70015a929c8d5`, additional
correctness fixes, native build failures, and full-scale stock fixtures. A
separate `unqualified-prepared-benchmark.patch` adds a proposed prepared-API
comparison mode to the existing benchmark target. That Rust mode has not
compiled or run. The existing Python protocol tests do not validate it.

The benchmark driver is committed and executable. Its protocol validation was
rerun on 2026-11-01: all 30 tests passed, including the 14 issue #530 tests.
The earlier included stock-only RSS smoke used C SQLite 3.53.2, 10 users,
100 posts, and 400 distinct likes under the explicitly labelled `four-per-post`
synthetic fixture. All 12 checked executions across six reported query families
matched that live oracle. These results validate the benchmark collector and
fixtures; they do not validate FrankenSQLite's candidate engine.

The [earlier qualification record](qualification-20261101.md) preserves the
preceding candidate and environment history; its patch hash and resource
snapshot are historical. Use the latest follow-up record for the current
patch identities, executed checks, raw logs, and remaining qualification work.

## Preserved engine work

`unqualified-engine-candidate.patch` contains the complete engine/test candidate
against checkpoint `bd569509d80427e11fb40d3654b70015a929c8d5`, whose relevant
engine files still match revision `770a812db0d494c46d900c5e5601768599d1bb34`.
Forward applicability to a clean snapshot of `770a812` and reverse applicability
to the modified working tree passed for both retained patches. Earlier candidate
versions remain in git history.
It is an unapplied patch: this checkpoint does not change engine behavior on main.

The candidate reuses indexed equality/IN loops for grouped input, with a narrow
covering-index path for plain COUNT(*) and stored grouping keys. It preserves
outer ordered-index traversal through lookup joins, bounds eligible streams
with LIMIT/OFFSET, admits validated indexed correlated COUNT(*) projections,
and derives relevant EXPLAIN QUERY PLAN output from actual cursor operations.
The new grouped-loop helper leaves the existing ungrouped aggregate emitter
unchanged. The follow-up fixes a case-sensitive aggregate-name eligibility bug
and admits selective equality groups with residual predicates while preserving
their table reads. Correlated-count admission excludes unsafe lazy expressions,
ambiguous outer names, projected-alias dependencies in WHERE/ON/LIMIT/OFFSET,
and unsupported VIRTUAL generated-column probes.

The patch includes rusqlite-oracle tests for binding, NULL/collation/affinity
boundaries, join multiplicity, retained prepared statements, transaction
rollback, overridden COUNT functions, aliases, and LIMIT-bounded work. Grouped
work guards include a forced full-scan control and zero table-row lookups for
the covering path. These Rust tests have not executed.

Static review, focused formatting checks, diff checks, and patch applicability
are not substitutes for compiler or runtime acceptance. Native qualification
remains blocked in unchanged Asupersync 0.5.0, before changed code has compiled.
The latest record distinguishes interrupted attempts, command setup failure,
and compiler processes killed under the shared memory limit.

The remaining optimization scope includes non-BINARY grouped predicates,
unfiltered ordered grouping, ordered LIMIT across three-or-more-table joins,
and generalized correlated expressions. The bounded sorter and ordered join
walk apply to the eligible two-table lookup path. Longer join paths retain
their existing unbounded sorter. Unsupported cases retain existing execution.

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

Apply `unqualified-prepared-benchmark.patch` identically to baseline and
candidate checkouts for the proposed prepared experiment. The latest record
contains full-scale job templates, seed recipes/hashes, matched-build
requirements, and the separate CLI timing/RSS commands. Run and validate the
new Rust benchmark before treating any output from it as qualification.

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

The preceding November 1 checkpoint had no unprivileged disk space. This
follow-up obtained disk space and the correct pinned compiler, but the worker
still shares an 8 GiB cgroup memory limit with unrelated builds. Native attempts
were killed while compiling unchanged Asupersync 0.5.0, before changed engine
code was reached. See the latest record for exact attempt outcomes and resource
snapshots. No dependency changes, incompatible compiled artifacts, source/cache
deletions, or weakened test gates were used to manufacture acceptance.
