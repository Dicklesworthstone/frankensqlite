# GH#526: composite EXISTS probes in trigger WHEN clauses

## Status and scope

The published 0.4.12 plan difference was reproduced on 2026-10-26. The source
change and regression target address the identified eligibility mismatch.
**Current-main executable tests and before/after timings have not yet run.**
Earlier builds were blocked by the host's memory limit; a recovered build
recipe is being investigated. No measured current-main speedup is claimed.

[Issue #526](https://github.com/Dicklesworthstone/frankensqlite/issues/526)
uses ten secondary indexes and 250 single-row updates in a transaction that is
rolled back. Its reported medians are below; these are the reporter's results,
not timings produced by this investigation.

| Rows | Reported 0.4.12 WHEN, ms/update | Reported 0.4.12 body, ms/update | Reported SQLite 3.46.1 WHEN/body, ms/update |
|---:|---:|---:|---:|
| 2,000 | 9.35 | 2.16 | 0.010 / 0.011 |
| 8,000 | 39.18 | 1.86 | 0.011 / 0.012 |
| 32,000 | 161.97 | 2.13 | 0.015 / 0.014 |

## Confirmed published-release diagnosis

The official [0.4.12 archive](https://github.com/Dicklesworthstone/frankensqlite/releases/download/v0.4.12/fsqlite-0.4.12-linux_amd64.tar.gz)
was checked against its published checksum. Its only member was inspected,
the extracted bytes were compared with that member, and the CLI reported
`fsqlite 0.4.12`. Archive SHA-256:

```text
12dcedd3d8c15bbac7d67ea83a25926b96000cad82e04a224112202f4224242b
```

Direct EXPLAIN probes used the issue's exact schema and 128 generated rows.
The normalized shape removes ORDER BY, as the nested EXISTS evaluator does.
These are compiled SELECT plans, not a runtime trace captured inside a firing
trigger. FrankenSQLite EXPLAIN does not execute the numbered parameters; the
manifest records their mapping and the example values supplied to stock.

| 0.4.12 probe | Index | Seek record fields | `IdxGT` prefix fields |
|---|---|---:|---:|
| Literal operands, original ORDER BY | x1 | 4 | 4 |
| Numbered operands, original ORDER BY | x1 | 4 | 4 |
| Literal operands, normalized EXISTS | x1 | 2 | 2 |
| Numbered operands, normalized EXISTS | x1 | **1** | **1** |

The normalized literal plan seeks `(c3,c4)`. The numbered plan seeks only
`c3`, whose generated values are `ns-a` and `ns-b`, and tests the other terms
for every candidate in that broad duplicate range. The decisive difference
is the usable prefix width, even though both forms select x1.

The numbered program also contains a fallback table scan, but it need not run
for this regression to be expensive. The candidate flag is set before residual
filtering; a native TEXT key also bypasses the fallback through `IsType`.
The broad `c3` index walk itself explains why matching probes can remain slow
and why implicit conversion or a zero-match fallback is not the whole cause.

The supplemental stock diagnostic used SQLite **3.53.1**. All four shapes
selected x10 with `(c1,c2,c3,c4,c5)` constrained. No timing comparison was made
with this published CLI diagnostic.

Retained text evidence: [manifest, schema and SQL](gh526-evidence/published-0.4.12-manifest.json),
[raw EQP](gh526-evidence/published-0.4.12-eqp.txt), and
[raw VDBE excerpts](gh526-evidence/published-0.4.12-vdbe-excerpts.txt).
No executable or database is included in the repository evidence.

## Architecture and prior work

The historical [port plan](../planning/PLAN_TO_PORT_SQLITE_TO_RUST.md) points to
the current [comprehensive specification](../planning/COMPREHENSIVE_SPEC_FOR_FRANKENSQLITE_V1.md).
Sections 12.1 (SELECT), 12.6 (CREATE INDEX), 12.8 (CREATE TRIGGER) and 12.16
(affinity), together with the [performance plan](../planning/PERFORMANCE_OPTIMIZATION_PLAN.md),
govern the access-path change and its measurement. Relevant closed work in
[the bead ledger](../../.beads/issues.jsonl) includes:

| Bead | Relevant existing behavior |
|---|---|
| `bd-ry6x7` | Parameterizes eligible WHEN subqueries for program reuse. |
| `bd-l9j27` | Reuses trigger-body programs; RAISE/subquery statements retain literals. |
| `bd-8c68u` | Extends parameterized guards to joined and scalar shapes. |
| `bd-nd2ju` | Addresses correlated trigger EXISTS and WITHOUT ROWID access paths. |
| `bd-joidf` | Makes proven zero-match partial/composite seeks authoritative. |
| `bd-gtjlv` | Extends equality scans beyond the leading index term. |
| `bd-420r8`, `bd-qteu2` | Address per-write work and full-row memdb rehydration. |

In [connection.rs](../../crates/fsqlite-core/src/connection.rs),
`trigger_when_matches` calls `bind_trigger_when_expr`. OLD/NEW references in
eligible subqueries become stable numbered parameters with current row values
supplied on execution. `bind_trigger_body_statement` keeps the equivalent
RAISE/subquery body literal-bound. The EXISTS evaluator executes a nested
SELECT; `strip_exists_subquery_projection` preserves this probe's LIMIT 1,
uses the constant projection, and removes its unnecessary ORDER BY.

In [codegen.rs](../../crates/fsqlite-vdbe/src/codegen.rs), the former
`literal_exists_index_prefix` selects a safe composite prefix before an
ordinary planner directive, but rejects every parameter. Its ordered-scan
emitter already supports runtime key affinity and a complete prefix boundary.
The binding/normalization path and the observed bytecode identify a concrete
source of unnecessary work. The fix extends prefix eligibility while keeping
the parameterized binder and compilation-cache protocol.

## Change and semantic boundaries

`exists_index_prefix` additionally admits bare numbered parameters for `=`.
Each key is loaded afresh, rejected if NULL, and given comparison affinity
before `SeekGE`. `IdxGT` checks the complete prefix after every index step;
the whole WHERE predicate still filters each candidate.

- Parameterized `IS` cannot extend this prefix: `NULL IS NULL` must remain true.
- Non-NULL literal `IS` remains supported; Boolean `IS` remains excluded.
- Partial/expression indexes, non-BINARY comparison/index collations and custom
  collation semantics keep their existing conservative gates.
- A DESC term ends prefix extension. Explicit hints, unsupported SELECT shapes,
  cast-wrapped bounds and uncanonicalized parameters keep their existing paths.
- Separate key registers preserve bindings when the same parameter receives
  different comparison affinities. Affinity does not mutate the binding itself.
- The change leaves trigger ordering, OLD/NEW binding, statement rollback and
  transaction execution in their existing paths.

For the issue's x1, the safe prefix remains `(c3,c4)` because c8 is descending.
Work can still grow with the number of rows sharing that prefix; this does not
establish constant time for arbitrary distributions or parity with stock's x10.

## Regression coverage and reproduction

[The integration target](../../crates/fsqlite-core/tests/gh526_trigger_when_composite_exists.rs)
contains the original schema, row generator and both trigger forms. Its tests
are designed to compare matching/missing probes at 128/2,048 rows with stock,
bound VM work and hydration, and preserve nullable residuals, changed OLD/NEW
keys, binding reuse, affinity, trigger order, ABORT, savepoints and rollback.
Codegen tests protect the composite prefix against a weak planner directive,
sparse and commuted parameters, the DESC boundary, lost residuals, fallback
table scans and accidental admission of nullable `IS` parameters.

Commands below assume a host with sufficient build capacity. Record any
toolchain/profile overrides and use identical settings for baseline/candidate;
earlier failed memory-fit attempts are not validated benchmark profiles.

```bash
export RUST_MIN_STACK=268435456
cargo test --locked --profile release-perf -p fsqlite-vdbe --lib gh526_ \
  -- --nocapture --test-threads=1
cargo test --locked --profile release-perf -p fsqlite-core \
  --test gh526_trigger_when_composite_exists -- --nocapture --test-threads=1
cargo test --locked --profile release-perf -p fsqlite-core \
  --test gh526_trigger_when_composite_exists \
  composite_trigger_when_benchmark_against_stock \
  -- --exact --ignored --nocapture --test-threads=1
```

The ignored benchmark defaults to 2k/8k/32k rows, 250 updates and three fresh
database repetitions for no-trigger/WHEN/body arms. Both engines run in one
invocation; cell order rotates and engine order alternates. Stock receives one
warmup. The FrankenSQLite arm uses the issue's batch API inside an explicit
transaction; stock prepares the identical UPDATE. All timed writes roll back.
`GH526_BENCH_ROWS`, `GH526_BENCH_UPDATES` and `GH526_BENCH_REPS` overrides must
be disclosed. This paired control uses three repetitions, whereas the issue's
stock timing table used five. The lockfile's Rust oracle is SQLite **3.53.2**.

## Validation status: receipts still required

The implementation and regression suite were committed directly to main in
[`14c7f7a196f4`](https://github.com/Dicklesworthstone/frankensqlite/commit/14c7f7a196f47804bcb5ce5af747c2cc56294c94),
on parent `82dcce30f051c18cf7424dfedf1a219904585c3d`, preserving intervening
unrelated work. A controlled future comparison should use those two revisions
and carry only the identical new test harness onto the parent revision.

The initial baseline was `4a60d9f7168cd1a3fc6943e5e2c7ad52dc6f89d0`;
`ac0c616166c6f40360ac2406a970f8540b9cc8fc` has the same source tree. After the
interrupted session, the checkout was recovered at
`226bf9f0392e0ef42fffed5b5b2f3c236b8ec839`, preserving its unrelated GH#527
change. A new before/after comparison must use the same baseline source.

Earlier builds died in unmodified asupersync 0.5.0, before compiling the changed
engine. Even a serial compiler attempt died during frontend work at 2,403,404
KiB peak RSS under the shared 8 GiB limit. The filesystem also became nearly
full. A previously successful low-memory recipe from repository history was
recovered, but only about 26 MB of actual free disk remained before another
compile could start. The [configuration](gh526-evidence/build-hints.toml) and
[build provenance](gh526-evidence/build-provenance.json) are retained. This
recovered recipe has not supplied a GH#526 test or timing receipt; its opt-level
0 settings are for diagnostic builds, not comparable release timings.

| Gate | Current evidence |
|---|---|
| Published 0.4.12 / stock EXPLAIN diagnostic | Reproduced; linked text evidence |
| Candidate codegen and stock-oracle tests | Not executed; awaiting a successful build |
| Existing trigger/index correctness regressions | Not executed |
| Same-profile baseline/candidate benchmark with stock | Not executed |
| Workspace check / Clippy in the earlier checkout | SIGKILL in asupersync |
| Earlier workspace `cargo fmt --check` | Diffs in 20 unchanged baseline files |
| Restored helper/test-fragment and integration-test rustfmt | Passed |
| Live stock-only SQL fixture/order/rollback checks | Passed on SQLite 3.53.1; not FrankenSQLite execution |
| Restored checkout `git diff --check` | Passed |

Useful existing targets are `bd_ry6x7_trigger_when_subquery_params`,
`trigger_timing_old_new_oracle`, `ordered_index_prefix_affinity_oracle` and
`bd_gtjlv_composite_eq_residual_prefix_seek`. Repository giant-file rustfmt
exclusions must remain respected. UBS and RCH were unavailable in the earlier
environment. Keep the issue open until executable
correctness and performance evidence exists; the [negative-results ledger](perf-negative-results.md)
also requires separating a narrow SQL improvement from an end-to-end consumer win.
