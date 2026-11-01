# Issue #531: benchmark validation and unqualified engine candidate

The benchmark driver is committed and executable. Its protocol validation passed
30 tests, including all 14 existing issue #530 protocol tests. The included
stock-only RSS smoke used C SQLite 3.53.2, 10 users, 100 posts, and 400 distinct
likes under the explicitly labelled `four-per-post` synthetic fixture. All 12
checked executions across six reported query families matched the live oracle.

These results validate the benchmark collector and fixtures. They do not
validate FrankenSQLite's candidate engine or establish a query speedup.

## Preserved engine work

`unqualified-engine-candidate.patch` contains the complete reviewed candidate
against engine revision `67478c6ccaa74a1dab2131a98692409e8f9f99be`. It applies
cleanly to that baseline and to the engine files accompanying this checkpoint.
It is an unapplied patch: the engine behavior on main has not changed.

The candidate reuses indexed equality/IN loops for grouped input, preserves
outer ordered-index traversal through lookup joins, bounds eligible streams
with LIMIT/OFFSET, admits validated indexed correlated COUNT(*) projections,
and derives relevant EXPLAIN QUERY PLAN output from actual cursor operations.
It includes live-rusqlite cases for binding, NULL/collation/affinity boundaries,
join multiplicity, retained prepared statements, transaction rollback, and
LIMIT-bounded work. Grouped-input work guards include a forced full-scan control.

Static review, focused formatting checks, diff checks, and patch applicability
are not substitutes for compiler or runtime acceptance. Earlier native build
and check attempts stopped in the unchanged Asupersync dependency because of
the shared worker's resource limits. No native test in this patch has run.

The remaining scope includes noncovering grouped seeks, non-BINARY grouping
fallback, unfiltered ordered grouping, and broader multi-source joins.

## Native qualification required before applying the engine changes to main

On a worker with sufficient disk and memory, apply the patch to a disposable
checkout of the exact engine baseline or this checkpoint. Use the repository's
pinned nightly and unchanged dependency lock:

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
Use the reported full-scale fixture for performance conclusions. Its CLI timing
includes parsing/preparation and result printing; it is not the reporter's
unattached prepared-API benchmark. Do not relabel process RSS, output rows, or
cursor-navigation opcode counts as exact rows examined.

## Current resource boundary

At this checkpoint the shared disk (63 GiB), RAM filesystem (4.9 GiB), and
small auxiliary temporary volume were full. The cgroup memory limit is 8 GiB.
Available cached Asupersync artifacts have incompatible dependency versions,
features, or compiler identities and were not substituted. No unrelated cache,
source, database, or evidence file was deleted to make room.
