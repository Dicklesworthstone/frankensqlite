# fsqlite-harness

Conformance test runner and verification harness for FrankenSQLite. This crate is **not published** (`publish = false`).

## Overview

This crate is the central verification and testing infrastructure for the FrankenSQLite project. It is intentionally more than "just tests" -- it contains reusable verification tooling, trace exporters, schedule exploration harnesses, and parity checking that other crates can call into from their own test suites.

The harness provides a wide range of verification capabilities including differential testing against C SQLite (via rusqlite), crash recovery parity verification, concurrent writer parity, WAL/journal parity, extension parity matrices, metamorphic testing, adversarial search, soak testing, and formal TLA+ model integration. It also includes CI gate infrastructure, coverage enforcement, and release certification.

This crate sits at the top of the fsqlite workspace dependency graph for testing purposes. It depends on the core `fsqlite` crate, `fsqlite-vfs`, `fsqlite-types`, `fsqlite-error`, and numerous utility crates. In dev-dependencies, it pulls in nearly every fsqlite crate for comprehensive cross-crate testing.

## Key Modules

- `differential_runner` / `differential_v2` - Run identical SQL against FrankenSQLite and C SQLite, comparing results
- `oracle` - Reference oracle for expected query results
- `crash_recovery_parity` / `cross_process_crash_harness` - Verify crash recovery behavior matches C SQLite
- `concurrent_writer_parity` / `lock_txn_parity` - Verify concurrent access and transaction semantics
- `wal_journal_parity` - Verify WAL and journal mode behavior parity
- `fault_vfs` / `fault_profiles` - Fault injection VFS for testing error handling paths
- `metamorphic` - Metamorphic testing: transform queries and verify invariant results
- `adversarial_search` - Adversarial/fuzz-like test generation
- `extension_parity_matrix` - Verify extension behavior matches C SQLite
- `soak_executor` / `soak_profiles` - Long-running soak tests
- `tcl_conformance` - TCL test suite conformance checking
- `ci_gate_matrix` / `ci_coverage_gate` / `confidence_gates` - CI pipeline quality gates
- `release_certificate` - Generate release readiness certificates
- `replay_harness` / `replay_triage` - Record and replay test executions
- `tla` - TLA+ formal model integration
- `bloodstream` / `impact_graph` / `closure_wave` - Dependency and impact analysis
- `benchmark_corpus` / `perf_loop` - Performance benchmarking infrastructure

## Dependencies (runtime)

- `fsqlite`, `fsqlite-error`, `fsqlite-types`, `fsqlite-vfs`
- `asupersync`, `serde`, `serde_json`, `sha2`, `xxhash-rust`, `parking_lot`, `tracing`

## Dependencies (dev)

- All fsqlite extension crates (`fsqlite-ext-json`, `fsqlite-ext-fts5`, `fsqlite-ext-fts3`, `fsqlite-ext-rtree`, `fsqlite-ext-session`, `fsqlite-ext-icu`, `fsqlite-ext-misc`)
- Core crates: `fsqlite-ast`, `fsqlite-btree`, `fsqlite-core`, `fsqlite-func`, `fsqlite-mvcc`, `fsqlite-pager`, `fsqlite-parser`, `fsqlite-planner`, `fsqlite-vdbe`, `fsqlite-wal`
- Testing utilities: `blake3`, `proptest`, `tempfile`, `toml`, `trybuild`, `rusqlite`

## License

MIT
