# Running the Performance & Correctness Harness

This document covers how to run the `fsqlite-e2e` test suite, where artifacts
land, and how to interpret the results.  For methodology details (warmup,
statistics, PRAGMA fairness) see [METHODOLOGY.md](METHODOLOGY.md).  For
troubleshooting and how to add new fixtures/workloads see
[TROUBLESHOOTING.md](TROUBLESHOOTING.md).

## Quick Reference

### Smoke Test (30-second infrastructure check)

```bash
cargo run -p fsqlite-e2e --bin e2e-runner -- run-smoke
```

Verifies: golden-copy integrity, FrankenSQLite backend, C SQLite backend,
workload-generator determinism, corruption injector, canonicalization pipeline,
and logging pipeline.  Exit code 0 = all checks passed.

### Full Test Matrix

```bash
cargo run -p fsqlite-e2e --bin e2e-runner -- run-all
```

Runs smoke + correctness + recovery tests sequentially.  Writes `results.json`
and `report.md` to the output directory.

### Criterion Benchmarks

```bash
cargo bench -p fsqlite-e2e
```

Runs all six Criterion benchmark harnesses (`e2e_bench`,
`write_throughput_bench`, `read_heavy_bench`, `large_txn_bench`,
`mixed_oltp_bench`, `concurrent_write_bench`).  Results land in
`target/criterion/`.

---

## Binaries

| Binary | Purpose |
|--------|---------|
| `e2e-runner` | Primary test runner (smoke, correctness, recovery, report) |
| `realdb-e2e` | Single-engine workload runner + corpus management |
| `profile-db` | Extract JSON metadata from golden database files |
| `e2e-dashboard` | TUI dashboard for interactive result viewing |
| `e2e-viewer` | TUI viewer for JSONL run records and benchmark summaries |
| `corruption-demo` | Guided corruption + recovery walkthrough demo (narrative or JSON) |

---

## e2e-runner Commands

### run-smoke

```bash
cargo run -p fsqlite-e2e --bin e2e-runner -- run-smoke
cargo run -p fsqlite-e2e --bin e2e-runner -- run-smoke --json
```

### run-correctness

```bash
cargo run -p fsqlite-e2e --bin e2e-runner -- run-correctness
cargo run -p fsqlite-e2e --bin e2e-runner -- run-correctness --filter canonicalization
```

Tests:
- `correctness_workload_replay` -- Replay deterministic oplog on C SQLite.
- `correctness_canonicalization` -- VACUUM INTO produces stable hashes.
- `correctness_logical_comparison` -- Three-tier comparison of identical DBs.

### run-recovery

```bash
cargo run -p fsqlite-e2e --bin e2e-runner -- run-recovery
```

Tests:
- `recovery_bitflip` -- Inject bit-flip, verify hash changes.
- `recovery_page_zero` -- Zero a page, verify hash changes.
- `recovery_header_zero` -- Zero the header, verify hash changes.

### run-all

```bash
cargo run -p fsqlite-e2e --bin e2e-runner -- run-all
cargo run -p fsqlite-e2e --bin e2e-runner -- run-all --json --output /tmp/e2e-results
```

### report

```bash
cargo run -p fsqlite-e2e --bin e2e-runner -- report --output /tmp/e2e-results
```

Reads a previous `results.json` and renders a Markdown summary.

### Global Options

| Flag | Effect |
|------|--------|
| `--verbose`, `-v` | TRACE-level logging |
| `--output <DIR>` | Output directory (default: `sample_sqlite_db_files/runs/<timestamp>`) |
| `--filter <PATTERN>` | Run only tests whose name contains `PATTERN` |
| `--json` | JSON output instead of human-readable Markdown |
| `--no-color` | Disable ANSI colors |
| `--golden <DIR>` | Golden database directory (default: `sample_sqlite_db_files/golden`) |
| `--seed <N>` | Override RNG seed (default: 42) |

---

## realdb-e2e Commands

### corpus scan

```bash
cargo run -p fsqlite-e2e --bin realdb-e2e -- corpus scan
cargo run -p fsqlite-e2e --bin realdb-e2e -- corpus scan --root /dp --max-depth 4
cargo run -p fsqlite-e2e --bin realdb-e2e -- corpus scan --header-only
```

Discovers SQLite databases under a root directory.

### corpus verify

```bash
cargo run -p fsqlite-e2e --bin realdb-e2e -- corpus verify
cargo run -p fsqlite-e2e --bin realdb-e2e -- corpus verify --checksums path/to/checksums.sha256
```

Recomputes SHA-256 hashes of golden copies and compares against
`sample_sqlite_db_files/checksums.sha256`.  Output per file:

| Prefix | Meaning |
|--------|---------|
| `OK` | Hash matches |
| `MISMATCH` | Hash differs (golden copy corrupted or updated) |
| `MISSING` | File listed in checksums but not on disk |

### run (single-engine workload)

```bash
# C SQLite, single writer
cargo run -p fsqlite-e2e --bin realdb-e2e -- run \
  --engine sqlite3 --db chinook --workload commutative_inserts

# FrankenSQLite with MVCC, scaling sweep, 5 repeats
cargo run -p fsqlite-e2e --bin realdb-e2e -- run \
  --engine fsqlite --db chinook --workload hot_page_contention \
  --concurrency 1,2,4,8 --repeat 5 --mvcc

# Pretty-print JSON + append JSONL log
cargo run -p fsqlite-e2e --bin realdb-e2e -- run \
  --engine sqlite3 --db chinook --workload mixed_read_write \
  --pretty --output-jsonl /tmp/results.jsonl
```

| Flag | Effect |
|------|--------|
| `--engine <sqlite3\|fsqlite>` | Required: which engine to run |
| `--db <ID>` | Database fixture identifier (name in `golden/` or full path) |
| `--workload <NAME>` | Preset: `commutative_inserts`, `hot_page_contention`, `mixed_read_write` |
| `--concurrency <N\|LIST>` | Worker count(s), comma-separated (default: 1) |
| `--repeat <N>` | Repetitions per concurrency level (default: 1) |
| `--mvcc` | Enable MVCC concurrent_mode (fsqlite only) |
| `--pretty` | Pretty-print JSON (default: compact JSONL) |
| `--output-jsonl <PATH>` | Append each run record to a JSONL file |

### bench

```bash
cargo run -p fsqlite-e2e --bin realdb-e2e -- bench --db chinook --preset all
```

Runs the benchmark matrix and prints `BenchmarkSummary` JSON to stdout.
Use `--output-jsonl` to append compact JSONL records and `--output-md` to
write a Markdown report rendered from the collected summaries.

### corrupt

```bash
cargo run -p fsqlite-e2e --bin realdb-e2e -- corrupt \
  --db chinook --strategy bitflip --count 10 --seed 42
cargo run -p fsqlite-e2e --bin realdb-e2e -- corrupt \
  --db chinook --strategy page --page 1 --seed 42
```

Injects deterministic corruption into a working copy and prints a before/after
SHA-256 proof (or `--json` for a structured report).

---

## profile-db

```bash
cargo run -p fsqlite-e2e --bin profile-db
cargo run -p fsqlite-e2e --bin profile-db -- --golden sample_sqlite_db_files/golden --pretty
cargo run -p fsqlite-e2e --bin profile-db -- --db chinook
```

Writes one JSON metadata file per golden database into
`sample_sqlite_db_files/metadata/`.

---

## Where Artifacts Land

| Artifact | Location |
|----------|----------|
| Golden database copies | `sample_sqlite_db_files/golden/*.db` |
| Golden checksums | `sample_sqlite_db_files/checksums.sha256` |
| Database metadata (JSON) | `sample_sqlite_db_files/metadata/*.json` |
| E2E run results | `sample_sqlite_db_files/runs/<timestamp>/results.json` |
| E2E run report | `sample_sqlite_db_files/runs/<timestamp>/report.md` |
| JSONL run records | Path specified by `--output-jsonl` |
| Criterion benchmark data | `target/criterion/<bench_name>/<function>/new/` |
| Criterion HTML reports | `target/criterion/report/index.html` |

---

## Interpreting Results

### Run Record (JSONL)

Each `realdb-e2e run` iteration emits a `RunRecordV1` JSON object.  Key
fields:

```
schema_version   "fsqlite-e2e.run_record.v1"
engine.name      "sqlite3" or "fsqlite"
fixture_id       Database identifier
workload         Preset name
concurrency      Worker count
report.wall_time_ms        Wall-clock time for the workload
report.ops_total           Total operations executed
report.ops_per_sec         Throughput
report.retries             SQLITE_BUSY retries
report.aborts              Transaction aborts
report.correctness.*       Correctness tier results (see below)
report.error               null if clean, error string otherwise
environment.capture_mode   "captured" by default, "suppressed" for profiler-only runs
run_mode.profile_only_effective   true when the emitted record is profiler-safe
run_mode.run_integrity_check      whether post-run integrity_check executed
run_mode.capture_environment_metadata
                                  whether host/environment probing ran
```

For lower-overhead profiler runs, `realdb-e2e run` supports:

- `--profile-only` to disable post-run `PRAGMA integrity_check` and suppress
  host probing in the emitted `environment` block.
- `--skip-integrity-check` and `--skip-environment-metadata` when those knobs
  should be controlled independently.

Default runs are unchanged: they still emit a full `RunRecordV1` with captured
environment metadata and integrity-check results. Profiler-safe records also
carry an explicit `run_mode` block so downstream scripts do not need to infer
mode from nullable correctness fields alone.

### Correctness Tiers

The E2E suite validates results at three tiers.  Each tier is progressively
more tolerant of legitimate implementation differences:

**Tier 1: `raw_sha256`** (informational)
- SHA-256 of the raw on-disk database bytes.
- Expected to differ even for logically identical DBs, because page layout,
  freelist state, and WAL/SHM sidecar content vary between implementations.
- A match here is a strong signal, but a mismatch is not a bug.

**Tier 2: `canonical_sha256`** (primary compatibility proof)
- SHA-256 after deterministic canonicalization: `PRAGMA wal_checkpoint(TRUNCATE)`
  followed by `VACUUM INTO` a fresh file.
- If this matches, the two engines produced byte-identical databases after
  removing implementation-specific layout differences.

**Tier 3: `logical`** (fallback)
- Compare logical content via deterministic validation queries (schema + table
  rows with stable ordering) and require `PRAGMA integrity_check` to return
  `ok` on both engines.
- Used when canonicalization is unavailable or mismatches.

### Dump Mismatch

A `dump_match: false` in the correctness report means the two engines produced
different logical content for the same workload.  To investigate:

1. Check the `report.error` field -- a mid-workload error may have caused
   partial execution.
2. Re-run with `--repeat 1` and `--pretty` to get a readable JSON report.
3. Compare the `correctness.logical_sha256` values between the two engines.
4. Look at `report.retries` and `report.aborts` -- high abort counts suggest
   the workload hits contention limits that may cause divergent outcomes.

### SHA Mismatch

A `raw_sha256_match: false` is normal and expected.  Only
`canonical_sha256_match: false` indicates a potential correctness issue.

If `canonical_sha256_match` is false:

1. Verify both engines ran the identical PRAGMA configuration (check the
   `methodology.identical_pragmas_enforced` flag in the report).
2. Confirm the workload is deterministic (same `seed` and `rng` in the report).
3. If `integrity_check_ok` is true for both engines, the divergence is likely a
   canonicalization edge case (freelist ordering, autovacuum state) rather than
   a data corruption bug.

### integrity_check Failure

`integrity_check_ok: false` means `PRAGMA integrity_check` returned something
other than `"ok"`.  This is the most serious failure type.

1. The database B-tree is internally inconsistent.
2. Re-run the same fixture + workload + seed to confirm reproducibility.
3. Check `report.error` for any execution errors that preceded the check.
4. If reproducible, file a bug with the full JSON report attached, including
   the `fixture_id`, `workload`, `seed`, `concurrency`, and
   `golden_sha256` fields for reproduction.

### Scaling Summary

When running with `--concurrency 1,2,4,8 --repeat N`, a scaling summary
table is printed to stderr:

```
------------------------------------------------------------------------
  Scaling summary: engine=sqlite3 repeat=5
------------------------------------------------------------------------
        Conc    p50 ops/s    p95 ops/s     p50 ms  p50 retries
  ------------------------------------------------------------------------
           1       1234.5       1100.2        810          0
           2       2100.0       1900.5        476          3
           4       3500.0       3100.0        286         12
           8       4200.0       3600.0        238         45
------------------------------------------------------------------------
```

- **p50 ops/s increasing** with concurrency = good scaling.
- **p50 retries climbing** = contention overhead.
- **ops/s flattening or dropping** = lock contention saturating throughput.

---

## Criterion Benchmarks

```bash
# Run all benchmarks
cargo bench -p fsqlite-e2e

# Run a specific benchmark
cargo bench -p fsqlite-e2e --bench e2e_bench

# Available benchmarks:
#   e2e_bench              Core E2E comparison (fsqlite vs C SQLite)
#   write_throughput_bench Write-heavy workloads
#   read_heavy_bench       Read-dominant workloads
#   large_txn_bench        Large transaction sizes
#   mixed_oltp_bench       Mixed OLTP patterns
#   concurrent_write_bench Concurrent writer scaling
```

Results land in `target/criterion/`.  Criterion generates HTML reports at
`target/criterion/report/index.html` with comparison charts when baseline
data exists from a previous run.
