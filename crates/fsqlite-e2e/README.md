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

## Benchmarks

- `e2e_bench` - General end-to-end benchmarks
- `write_throughput_bench` - Write throughput measurement
- `read_heavy_bench` - Read-heavy workload benchmarks
- `large_txn_bench` - Large transaction benchmarks
- `mixed_oltp_bench` - Mixed OLTP workload benchmarks
- `concurrent_write_bench` - Concurrent write benchmarks
- `operation_baseline_bench` - Operation-level baseline benchmarks

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
