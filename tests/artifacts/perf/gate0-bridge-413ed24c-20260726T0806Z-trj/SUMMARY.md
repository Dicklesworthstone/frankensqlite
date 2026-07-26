# Gate 0 same-source runtime-bridge diagnostic

> **NON-CITABLE DIAGNOSTIC EVIDENCE**
>
> All four benchmark processes completed and passed the artifact's structural
> validator, but every report sets `provenance.citable` to `false`. Do not use
> these numbers in README, release, or comparative performance claims.

## Question

This experiment separates three ways of driving the same async engine source:

1. `inside_existing_runtime`: enter one reused asupersync runtime outside the
   timer, then await every timed operation inside it.
2. `per_operation_block_on`: call the reused thread-local
   `Runtime::block_on` once for every timed operation.
3. `worker_sync_facade`: use the public `AsyncConnection` synchronous facade,
   including SQL/parameter cloning, response-channel allocation, worker-channel
   transit, worker scheduling, and `futures_lite::block_on`.

It also runs a ready-future control and compares the throughput-oriented
`release-perf` profile (`opt-level=3`) with the shipped `release` profile
(`opt-level=z`). This is a same-source mechanism experiment. It is not a
historical async-migration A/B.

## Frozen identity

- Source:
  `413ed24cd0ff8dcfa2bbc7ed395dd1cf5db4e25f`
- Host: Threadripper PRO 5995WX (`trj`), pinned to CPUs `2-3`
- Toolchain: `rustc 1.99.0-nightly (008fa22ce 2026-07-25)`,
  `cargo 1.99.0-nightly (3efb1f477 2026-07-17)`
- `release-perf` binary:
  `b7b56a13d962d554480e9943c09aebe5a477837414b752e5fa418649734d94d0`,
  25,641,168 bytes
- `release` binary:
  `cced2e9b035cf6bb41ecb65b84a6c219288bcfcf4ebb7bf57e4828d5d76290d2`,
  17,736,368 bytes

Both builds used an isolated target directory, explicit
`x86_64-unknown-linux-gnu` target, empty Cargo/Rust compiler wrappers and
encoded rustflags, LTO, one codegen unit, `panic=abort`, stripping, a unique
build nonce, and a captured `cargo -vv` event/log receipt. The v2 bridge schema
was byte-identical between profiles.

## Design and validation

- Outer profile order: ABBA — `release-perf`, `release`, `release`,
  `release-perf`
- Seeds: `2026072601` and `2026072602`, with the profile order reversed between
  seed replications
- 48 samples per arm per process
- 1,000 operations per prepared/raw sample
- 624 retained raw samples and 72 host checkpoints per process
- 2,496 retained raw samples and 288 checkpoints in total
- Exact cluster bootstrap over complete design clusters
- Exact two-sided sign randomization within each process
- Both 95% and Bonferroni 99.1667% intervals retained in
  `analysis/exact-statistics.jsonl`

`analysis/structural-validation.json` records a pass for every process:
schema, seed, profile, source SHA, binary identity, log identity, disabled
statement tracing, enabled concurrent-writer default, matched PRAGMAs,
direct-DML routing probes, row/checksum oracles, exact counters, order balance,
and host-state receipt structure all match the declared contract.

## Why the result is non-citable

Each report contains exactly 224 provenance errors:

- SBH was active before, during, and after measurement.
- The one-minute host load exceeded the declared ceiling of `1.0`.
- I/O PSI `some avg60` exceeded the declared ceiling of `0.100`.
- The bridge runner does not yet have a fail-bounded external watchdog.
- The run lacks an isolated cpuset/full-dynticks/IRQ/per-thread-frequency
  receipt.

There were no unexpected source/build/routing/tracing/oracle errors. The
explicit provenance override preserves a useful diagnostic while preventing it
from silently becoming release evidence.

## Diagnostic results

Ratios below are replicated geometric means across the two independent seed
runs. Differences are the mean nanoseconds per operation across those runs.
They are descriptive diagnostic estimates, not release claims.

| Profile | Workload | Contrast | Ratio | Difference |
|---|---|---:|---:|---:|
| `release-perf` | prepared INSERT | per-op / inside runtime | 2.2003x | +621.7 ns/op |
| `release-perf` | raw `execute_with_params` INSERT | per-op / inside runtime | 1.4866x | +441.9 ns/op |
| `release-perf` | raw INSERT | worker / inside runtime | 9.5134x | +7,719.6 ns/op |
| `release-perf` | raw INSERT | worker / per-op | 6.3993x | +7,277.7 ns/op |
| `release` | prepared INSERT | per-op / inside runtime | 2.2946x | +1,177.1 ns/op |
| `release` | raw `execute_with_params` INSERT | per-op / inside runtime | 1.5383x | +840.0 ns/op |
| `release` | raw INSERT | worker / inside runtime | 6.6099x | +8,749.1 ns/op |
| `release` | raw INSERT | worker / per-op | 4.2968x | +7,909.1 ns/op |

The ready-future runtime-entry slope was 217.1 ns/entry in `release-perf` and
316.1 ns/entry in `release`. That control accounts for only part of the
prepared/raw bridge gap. The measured mechanism is therefore repeated runtime
entry plus the scheduler/future interaction of the real operation, not merely
runtime construction or one constant entry function.

The outer-profile diagnostic compares `release` with `release-perf`:

| Workload and arm | `release` / `release-perf` |
|---|---:|
| prepared INSERT, inside runtime | 1.7538x |
| prepared INSERT, per-op bridge | 1.8291x |
| raw INSERT, inside runtime | 1.7206x |
| raw INSERT, per-op bridge | 1.7802x |
| raw INSERT, worker facade | 1.1952x |

The `release` binary was 30.8% smaller, but the selected ordinary engine arms
were roughly 1.72-1.83x slower. With only two profile-level seed replications,
these are point estimates, not an inferential release-profile verdict.

## Conclusions supported by this artifact

1. One `block_on` entry per operation is a large, repeatable tax for these
   same-source write paths. The primary engine-throughput benchmark must enter
   one runtime per scenario or batch, while the sync-adapter cost remains a
   separately scored surface.
2. The current worker facade is not a sync-path performance fix. It measures a
   larger system boundary and is much slower than both direct arms here.
3. `opt-level=z` is a serious distribution-performance risk and needs an
   explicit size/throughput Pareto matrix. This artifact does not by itself
   choose between `opt-level=3`, hot-crate overrides, or a separate
   size-optimized profile.
4. The historical broad regression is not fully attributed. In particular,
   this artifact does not prove that boxed `WalBackend` futures caused it.
   Allocation and CPU profiles plus a one-variable A/B are still required.

## Recommended sequence

1. Keep the release held for the independently identified cancellation,
   transaction-drop, fence-cleanup, and exactly-once accounting defects in
   `bd-wymdl`.
2. Make one-runtime-per-scenario the primary FrankenSQLite throughput arm and
   retain per-operation sync bridging as an explicit adapter benchmark.
3. Add a batched/closure-based sync entry point so synchronous callers can
   amortize runtime entry across a transaction or operation batch.
4. Profile the residual per-op-versus-inside-runtime delta in CPU and
   allocation space before changing WAL dispatch or future representation.
5. Run the shipped-profile Pareto matrix with isolated-host provenance and
   decide whether normal release should be throughput optimized, whether hot
   engine crates need overrides, or whether a separate `release-small` profile
   is warranted.
6. Re-run the historical comprehensive matrix only after it reports the
   async-native and sync-adapter surfaces separately.
7. Address recursive execution with an explicit work stack/trampoline and a
   pinned stack-size matrix; compiler `recursion_limit` and the optional
   16 MiB worker stack are not raw-API stack-safety proofs.

## Artifact map

- `analysis/runs/*/report.json`: four complete v2 reports with retained samples
- `analysis/exact-statistics.jsonl`: exact within-run inference
- `analysis/diagnostic-summary.json`: replicated descriptive aggregation
- `analysis/structural-validation.json`: independent structural checks
- `analysis/run-order.tsv`: outer ABBA order
- `builds/{release,release-perf}/`: binary hashes, build nonces, v2 schemas,
  Cargo event streams, and verbose build receipts
- `MANIFEST.sha256`: hash manifest for every other artifact file
