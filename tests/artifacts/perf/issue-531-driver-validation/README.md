# Issue #531: selective GROUP BY implementation and validation

## Implemented on main: indexed grouped input (2026-11-02)

The VDBE grouped-aggregate generator now reuses selective equality-prefix and
constant-IN index probes instead of unconditionally scanning the table. A narrow
covering path reads stored INTEGER/TEXT grouping keys and plain COUNT(*) directly
from the index. Residual predicates and richer aggregates retain table reads.
Unsupported collation, affinity, binding, and index-coverage cases keep their
existing execution path. The ungrouped aggregate generator is unchanged.

The permanent compiler regression target is
`crates/fsqlite-vdbe/tests/issue_531_grouped_index_input.rs`. The same ten
assertions were compiled and executed against real production codegen,
ProgramBuilder, parser, and locked leaf dependencies:

| Source | Passed | Failed |
| --- | ---: | ---: |
| Unchanged engine baseline `770a812d` | 3 | 7 |
| Initial isolated candidate | 7 | 3 |
| Corrected implementation in this commit | 10 | 0 |

Actual execution caught a case-sensitive COUNT admission bug in the initial
candidate. Aggregate parsing normalizes the name to uppercase; the implementation
now uses a case-insensitive comparison. The tests were unchanged between runs.

The passing source SHA256 is
`55b8ad8b05754035de1624b17984f76ff534005f6defecefb3424a9a40351974`.
The test source SHA256 is
`845c395d06138fde7286ec81f7d394c12f3635af294623bbb22122db4d0cc3e2`.

[grouped-codegen-20261102.tar.gz](grouped-codegen-20261102.tar.gz) preserves the
exact harness, source manifests, commands, resource guards, and raw before/after
logs. Its SHA256 is
`759c4bb578598968c7e0734565f63f8a6c99f3fba0c023eee99849e3e35ebd04`.
Start with `group-focused-result.json`, `group-focused-provenance.json`, and
`evidence-index.json` inside the archive. The harness includes unchanged real
production source sections; it contains no mock VM or substituted dependency.

### Validation scope and outstanding gates

These are native compiler/bytecode regressions. They do not execute the SQL
storage runtime. They establish selective seeks, covering cursor behavior,
binding positions, and conservative fallback in emitted programs; they do not
establish SQL result equivalence, transaction visibility, concurrency safety,
or measured query speedup versus C SQLite.

The added core regressions in `bd_group_by_depth_oracle_probe.rs` cover live
rusqlite comparison, retained prepared statements, pending writes and rollback,
collation/affinity/NULL boundaries, and VM-work budgets against a forced scan.
Those core tests still require execution. Full workspace check, clippy, and
runtime qualification remain blocked by the unchanged Asupersync dependency
build and shared worker resource limits. Compiler elapsed times are not query
benchmarks.

Correction to the historical formatter receipt: repository formatting rules
exclude the giant engine source trees. That check was not proof that those
files were formatted. The small permanent regression files were explicitly
formatted with the ignore list disabled; production formatting rules were not
changed.

## Remaining join, correlated-subquery, and EXPLAIN work

The independently committed [follow-up record](qualification-20261102.md)
and `followup-20261102/` preserve additional reviewed candidate work, both
full-scale stock fixtures, bound-query checks, build provenance, and native
build failures. These files are retained unchanged. The earlier
[qualification record](qualification-20261101.md) is also historical.

`unqualified-engine-candidate.patch` is the broader candidate preserved in
that follow-up against checkpoint `bd569509`, whose engine sources match
`770a812db0d494c46d900c5e5601768599d1bb34`. It includes grouped work now partly
implemented here as well as separate grouped refinements and still-unqualified
ordered-join, correlated-count, and EXPLAIN changes. The passing source identity
above identifies the narrower implementation actually committed. Do not apply
the complete historical patch to current main.

`unqualified-prepared-benchmark.patch` is a proposed prepared-API comparison
mode. It remains uncompiled and unexecuted. Apply and validate that overlay
identically in separate baseline and candidate checkouts before using its
output; the existing Python protocol tests do not qualify the new Rust mode.
The follow-up record contains the full-scale job templates and seed hashes.

The next priority is preserving ordered index traversal and LIMIT/OFFSET through
eligible lookup joins, followed by safe indexed correlated COUNT projections
and execution-faithful EXPLAIN reporting. Broader multi-source joins,
non-BINARY grouped probes, unfiltered ordered grouping, and generalized
correlated expressions remain outside the narrow grouped optimization.

## Runtime and performance reproduction

The benchmark driver is committed and executable. Its protocol validation on
2026-11-01 passed all 30 tests, including the 14 issue #530 tests. The earlier
stock-only smoke used C SQLite 3.53.2 with 10 users, 100 posts, and 400 distinct
likes under the explicitly labelled `four-per-post` synthetic fixture. All 12
checked executions across six query families matched that live oracle. This
validates the collector and fixture, not the modified FrankenSQLite runtime.

Use the pinned nightly and unchanged lockfile on a worker with adequate space:

```sh
cargo test --locked -p fsqlite-vdbe --test issue_531_grouped_index_input
cargo test --locked -p fsqlite-core --test bd_group_by_depth_oracle_probe
cargo check --locked --workspace --all-targets
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo fmt --all --check
python3 scripts/benchmark_issue_531.py --help
```

For matched baseline/candidate/C SQLite timing and separate Linux RSS runs,
provide verified binary identities and matching compiler/profile/features/
flags/dependency locks. Run both labelled full-scale fixture modes: the supplied
CLI recipe attempts two million likes but produces 500,000 distinct primary
keys; `four-per-post` constructs two million distinct synthetic likes. CLI
timing includes parsing/preparation and result printing and is not the
reporter's unattached prepared-API benchmark. Do not relabel process RSS, output
rows, or cursor-navigation opcode counts as exact rows examined.
