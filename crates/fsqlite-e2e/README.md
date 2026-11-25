# fsqlite-e2e

End-to-end differential testing and benchmark harness for FrankenSQLite. This crate is **not published** (`publish = false`).

## Overview

This crate provides the infrastructure for running full end-to-end tests that compare FrankenSQLite against C SQLite (via rusqlite). It handles golden copy management (loading, hashing, and comparing database snapshots), deterministic seeded workload generation, differential SQL execution, corruption injection for recovery testing, and performance benchmarking.

All E2E scenarios use deterministic seeding (default seed: `FRANKEN_SEED`) to ensure exact reproducibility. Each test execution can be replayed by specifying the same seed via `--seed <u64>` or the `E2E_SEED` environment variable.

This crate sits at the very top of the fsqlite workspace dependency graph. It depends on `fsqlite`, `fsqlite-core`, `fsqlite-harness`, `fsqlite-vfs`, `fsqlite-wal`, `fsqlite-types`, `fsqlite-error`, and `rusqlite` for the C SQLite reference implementation.

## Key Types

- `HarnessSettings` - Configuration struct ensuring identical pragma settings (journal mode, synchronous, cache size, page size, busy timeout, concurrent mode) are applied to both FrankenSQLite and C SQLite runs
- `FRANKEN_SEED` - Canonical default seed (`0x0046_5241_4E4B_454E`, "FRANKEN" as ASCII bytes) for all E2E scenarios

## Key Functions

- `derive_worker_seed(base_seed, worker_id)` - Derive a deterministic per-worker seed using golden ratio hashing
- `derive_scenario_seed(base_seed, scenario_hash)` - Derive a deterministic per-scenario seed

## Binaries

- `realdb-e2e` - Main end-to-end differential test runner
- `profile-db` - Database profiling tool
- `e2e-dashboard` - TUI dashboard for monitoring E2E runs (uses `ftui` with crossterm)
- `e2e-runner` - Batch E2E test execution
- `e2e-viewer` - View and inspect E2E test results
- `corruption-demo` - Demonstrate corruption injection and recovery
- `mt-mvcc-bench` - Honest shared-file multi-threaded concurrent writer snapshot

## Benchmarks

- `e2e_bench` - General end-to-end benchmarks
- `write_throughput_bench` - Write throughput measurement
- `read_heavy_bench` - Read-heavy workload benchmarks
- `large_txn_bench` - Large transaction benchmarks
- `mixed_oltp_bench` - Mixed OLTP workload benchmarks
- `concurrent_write_bench` - Sequential-control concurrent write benchmark (not MVCC proof)
- `concurrent_write_persistent_bench` - File-backed persistent concurrent-writer Criterion benchmark
- `operation_baseline_bench` - Operation-level baseline benchmarks

## Benchmark Discipline

Performance comparisons in this crate must be structurally fair:

- If the rusqlite side uses a prepared statement in a hot loop, the FrankenSQLite side must do the same.
- If the rusqlite side uses stable parameterized SQL, the FrankenSQLite side must not use per-iteration `format!()` SQL strings.
- If a FrankenSQLite benchmark is only a control and does **not** exercise the real persistent concurrent-writer path, its name and comments must say so explicitly.
- `mt-mvcc-bench` is the canonical 4/8/16-thread shared-file proof path for concurrent-writer claims. Do not cite `concurrent_write_bench` as MVCC win evidence.
- Benchmark reports should always state whether the path is prepared vs ad hoc, file-backed vs in-memory, and concurrent vs sequential control.

## Canonical Concurrent-Writer Snapshot

Use this command shape when you need the honest shared-file multi-threaded snapshot:

```bash
rch exec -- env CARGO_TARGET_DIR=${TMPDIR:-/tmp}/rch_target_<name> \
  cargo run -p fsqlite-e2e --bin mt-mvcc-bench -- \
  --threads=4,8,16 \
  --rows-per-thread=250 \
  --iters=1 \
  --json-output tests/artifacts/perf/concurrent-showcase-20260424/mt-mvcc-bench.json \
  --summary-md tests/artifacts/perf/concurrent-showcase-20260424/mt-mvcc-bench.md
```

Current blocker snapshot from [tests/artifacts/perf/concurrent-showcase-20260424/mt-mvcc-bench.md](/data/projects/frankensqlite/tests/artifacts/perf/concurrent-showcase-20260424/mt-mvcc-bench.md):

| Threads | fsqlite p50 wps | sqlite p50 wps | Throughput ratio | fsqlite p50 ms | sqlite p50 ms | Time ratio |
|---------|-----------------:|---------------:|-----------------:|---------------:|--------------:|-----------:|
| 4 | 3178 | 212016 | 0.01x | 314.68 | 4.72 | 66.72x |
| 8 | 6125 | 36152 | 0.17x | 326.55 | 55.32 | 5.90x |
| 16 | 5181 | 6325 | 0.82x | 772.04 | 632.44 | 1.22x |

Those numbers are a blocker, not evidence of an MVCC throughput win yet.

## OLTP Short-Transaction Shape (bd-acuhw)

Group commit can only coalesce when commits from different connections arrive
together, so it needs many short transactions per connection. One bulk
transaction per writer never exercises it. `mt-mvcc-bench` measures this shape
with two flags on top of `--one-row-per-transaction`:

```bash
rch exec -- env CARGO_TARGET_DIR=${TMPDIR:-/tmp}/rch_target_<name> \
  cargo run --profile release-perf -p fsqlite-e2e --bin mt-mvcc-bench -- \
  --one-row-per-transaction --rows-per-transaction=5 --stock-begin=immediate \
  --synchronous=full --separate-tables \
  --threads=1,2,4,8,16 --rows-per-thread=500 --iters=5
```

- `--rows-per-transaction=K` makes every transaction insert K consecutive
  rows. It requires `--one-row-per-transaction`, and K must divide
  `--rows-per-thread`. The report's `transaction_contract` receipt records K.
- `--stock-begin=immediate` opens the stock arm's transactions with
  `BEGIN IMMEDIATE`. FrankenSQLite keeps its default concurrent mode.
- Without these flags the run keeps the v10 one-row contract: K = 1, and stock
  opens with a deferred `BEGIN`.
- Per arm, stderr carries an executed-work receipt for both engines:
  `arm_cost engine=… threads=… rows_per_transaction=… committed_rows=…
  transactions=… workload_ms=… process_cpu_ms=… cpu_cores=…
  cpu_us_per_transaction=… os_threads_peak=…`. It is diagnostic only, and no
  verdict reads it.
- Per FrankenSQLite round, `group_commit_batches threads=… epochs=…
  transactions_batched=… mean_batch=… histogram=…` prints the epoch batch-size
  histogram from fsqlite-wal's `ConsolidationMetrics`. Coalescing shows up as
  `mean_batch` > 1. At `--synchronous=normal` no per-commit sync is shared, so
  expect batches of 1 there.

For the measured result, the method (same-worker A/B with an A/A null, stock
3.54.1 live) and the coverage limits, see the 2026-11-25 bd-6a8a5 L1 entry in
`docs/progress/perf-negative-results.md` and commit `c71e4a99e`.

## Canonical Hot-Profile Workflow

The standard way to answer “where is FrankenSQLite spending time?” is `realdb-e2e hot-profile`, not ad hoc speculation.

Example:

```bash
rch exec -- cargo run -p fsqlite-e2e --bin realdb-e2e -- hot-profile \
  --db beads-proj-a \
  --workload mixed_read_write \
  --concurrency 4 \
  --seed 42 \
  --scale 50 \
  --output-dir artifacts/perf/hot-profile/mixed_read_write_c4 \
  --mvcc \
  --pretty
```

This emits structured hot-path artifacts that break execution down by parser, rewrite/compile, VDBE, record decode, retries/conflicts, and other subsystems. Use it before proposing optimization work.

To sanity-check benchmark code changes without waiting on the full workspace, run:

```bash
rch exec -- cargo check -p fsqlite-e2e --benches
```

## Key Modules

- `golden` - Golden copy management and snapshot comparison
- `workload` - Deterministic workload generation
- `comparison` - Differential result comparison
- `corruption` / `corruption_scenarios` / `corruption_demo_sqlite` - Corruption injection at byte, page, and sector levels
- `executor` / `fsqlite_executor` / `sqlite_executor` - SQL execution engines for both FrankenSQLite and C SQLite
- `benchmark` / `perf_runner` / `bench_summary` - Benchmarking infrastructure
- `recovery_runner` / `recovery_demo` / `fsqlite_recovery_demo` - Recovery testing
- `report` / `report_render` - Test report generation and rendering
- `validation` / `verification_gates` - Result validation and quality gates
- `smoke` / `ci_smoke` - Quick smoke tests for CI

## Dependencies (runtime)

- `fsqlite`, `fsqlite-core`, `fsqlite-error`, `fsqlite-harness`, `fsqlite-vfs`, `fsqlite-types`, `fsqlite-wal`
- `rusqlite`, `ftui`, `rand`, `sha2`, `serde`, `serde_json`, `tempfile`, `thiserror`, `tracing`, `tracing-subscriber`

## Dependencies (dev)

- `criterion` (benchmarks), `jsonschema`, `fsqlite-ext-session`, `fsqlite-mvcc`

## License

MIT
