# Benchmark methodology: concurrent writers

## TL;DR

`crates/fsqlite-e2e/src/bin/comprehensive_bench.rs::bench_concurrent_writers`
runs FrankenSQLite and C SQLite with the same shape: N OS threads, one
connection per thread, one shared file-backed WAL database, disjoint rowid
ranges, and — since 2026-07-25 — the same `synchronous=NORMAL` durability on
both engines' writer connections.

**Do not cite its rows for concurrent-writer speed claims.** Even with the shape
and durability matched, the section is file-WAL disk-noise-bound: on a shared
host the C-side 2-writer median has been observed spreading 95-138 ms (CV up to
104 % at 8 writers), which is larger than the effects being compared. Use
`crates/fsqlite-e2e/src/bin/mt_mvcc_bench.rs` (IMPL-4a) for every published
concurrent-writer number: 1/2/4/8/16-thread reports, separate-table mode,
startup diagnostics, higher iteration counts, executable self-identification,
same-invocation C/C null controls, and bootstrap median-CI decisions.
Pass-over-pass history, CV, and MAD are diagnostics only; none can veto or
create a verdict.
See `docs/progress/perf-negative-results.md` (2026-07-23 / 2026-07-25, bd-x5gzk)
for the full evidence and the retry predicate.

## Background

FrankenSQLite's `Connection` type is `!Send + !Sync` — its internal state
includes `RefCell` and `Rc` fields that cannot cross thread boundaries.
For a benchmark to run true concurrent writers, it must construct
*one Connection per OS thread*, each bound to the same file-backed
database, and coordinate them at the MVCC/WAL layer below the Connection
API.

`bench_concurrent_writers` was originally written against the `rusqlite`
baseline, which *does* have a `Send` Connection. When the FrankenSQLite
baseline was first added, the loop iterating 1..N "writers" was left as a
sequential for-loop over a single Connection, with each "writer" performing a
transaction serially. Those older artifacts are apples-to-oranges and should
not be used for current MVCC claims.

The current implementation has been corrected: each FrankenSQLite worker opens
its own `Connection::open(path)` inside its worker thread, enables concurrent
mode, and runs `BEGIN CONCURRENT` against the same file-backed database. That
matches the current C SQLite WAL arm's one-connection-per-thread shape.

## Why this matters

Several optimization items in the current campaign (IMPL-4 flat-combining
page lock table, IMPL-14 Cicada read-ts batching, IMPL-15 Hekaton TID
gap reservation, IMPL-16 Silo epoch group commit, IMPL-24 MICA
partitioned commit log) target multi-writer contention. Older
`bench_concurrent_writers` artifacts cannot measure those accurately. Current
artifacts can, but `mt_mvcc_bench` remains the preferred focused harness when
the optimization is specifically about concurrent writer scaling.

- `IMPL-4` (flat-combining) was **refused** by the implementing agent
  after discovering that the feature was already wired behind
  `mvcc-flat-combining` and that the bench could not observe any
  difference because writers were sequential.
- Apparent "4.72× faster at 8 writers" in earlier reports was not a
  FrankenSQLite win — it was a sequential-vs-multi-threaded comparison
  that happened to favor the sequential side under low per-op cost.

## What IMPL-4a provides

`mt_mvcc_bench` spawns N OS threads, each opening its own
`Connection::open(path)` against a shared file-backed database, each
running BEGIN CONCURRENT (or BEGIN for fallback), and each committing a
fixed number of rows. It measures wall-clock throughput and compares
against a matched rusqlite WAL-mode workload.

The numbers it reports are directly comparable because both sides run
the same count of OS threads performing the same count of transactions.

Since 2026-07-26, one thread-count measurement consists of four independent
fresh-database arms per paired round: C null A, C null B, C baseline, and
FrankenSQLite candidate. Odd rounds reverse the execution order. The report
bootstraps the median of the per-round ratios 10,000 times and calls a result
only when the claim interval clears twice the measured C/C null radius, with a
minimum 1% effect. A high CV is printed as provenance and is never a gate.

The benchmark program's first stdout line reports the SHA-256, byte length, and
path of the executable that is actually running. The next identity line reports
the benchmark source SHA-256. Record both with every published row; a Cargo
target path or Git HEAD inferred by the launching shell is not a substitute for
the running ELF's identity.

## When to use which bench

| Use case | Use |
|---|---|
| Single-connection latency | `comprehensive_bench::bench_*` (all but concurrent_writers) |
| Full-matrix concurrent row (smoke only — do not cite) | `comprehensive_bench::bench_concurrent_writers` |
| Real multi-thread MVCC throughput | `mt_mvcc_bench` (IMPL-4a) |
| Cross-process conflict | `swarm_multiprocess` / `swarm_peer_visibility` |

## Before you modify `bench_concurrent_writers`

Keep it aligned with `mt_mvcc_bench`: one connection per worker thread, shared
file-backed database for shared-table mode, disjoint rowid ranges, prepared
statements on both engines, and transaction-level retry for transient MVCC
errors. If you change its workload shape, update this document and the README
performance artifact citations in the same commit.

**Matched durability is an invariant, not a detail.** `synchronous` is a
*per-connection* pragma — the setup connection's setting does NOT carry to the
worker connections. A C SQLite connection that never sets it inherits the
compiled default `SQLITE_DEFAULT_SYNCHRONOUS=2` (`FULL`), i.e. a real WAL fsync
on every commit, while FrankenSQLite's `NORMAL` maps to
`WalCommitSyncPolicy::Deferred` and does no per-commit fsync at all. That
asymmetry silently flattered FrankenSQLite for the life of the section until it
was found in 2026-07-23 (bd-x5gzk). Both writer arms now set
`synchronous=NORMAL` explicitly. `FSQLITE_BENCH_CONCURRENT_SYNC=normal|full`
forces both engines to the named level together; it must never be used to change
only one side.

## Current reference invocation

The 2026-07-26 Lane-M reference run used:

```bash
RCH_WORKER=vmi1149989 RCH_REQUIRE_REMOTE=1 env -u CARGO_TARGET_DIR \
  rch exec -- cargo run --profile release-perf -j7 -p fsqlite-e2e \
  --bin mt-mvcc-bench -- --rows-per-thread=1000 \
  --threads=1,2,4,8 --iters=21

RCH_WORKER=vmi1149989 RCH_REQUIRE_REMOTE=1 env -u CARGO_TARGET_DIR \
  rch exec -- cargo run --profile release-perf -j7 -p fsqlite-e2e \
  --bin mt-mvcc-bench -- --rows-per-thread=1000 \
  --threads=1,2,4,8 --iters=21 --separate-tables
```

The complete null and claim intervals, exact ELF identities, verdicts, and
retry predicates are recorded in
`docs/progress/perf-negative-results.md` under the 2026-07-26 matched-sync
entry. The README reproduces only the compact result tables.

## Related

- Campaign memory: `session_2026_04_18_ag_aac_campaign.md` — INSIGHT #75
- Blocked-by: IMPL-4, IMPL-14, IMPL-15, IMPL-16, IMPL-24
