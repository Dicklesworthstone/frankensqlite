# `bd-1eog` RaptorQ Microbench Matrix

This note records the microbenchmark matrix and how to use it to justify
default RaptorQ settings (`T` and repair overhead).

## Scope

Implemented benchmark target:

- `crates/fsqlite-wal/benches/raptorq_matrix.rs`

Measured dimensions:

- `K_source`: representative points across small/medium/large (`1`, `8`, `32`, `256`, `1024`, `4096`)
- `T`: MTU-ish (`1366`) and page-ish (`4096`)
- loss rate: `0%`, `5%`, `10%`, `20%`

Measured outputs:

- systematic fast-path throughput (bytes/s)
- repair-symbol generation throughput (bytes/s)
- decode throughput (bytes/s)
- decode completion latency summaries (`p95`, `p99`, nanoseconds)
- hash verification and symbol-auth verification cost

## How To Run

Smoke mode (CI-friendly, deterministic subset):

```bash
FSQLITE_BENCH_SMOKE=1 cargo bench -p fsqlite-wal --bench raptorq_matrix -- --noplot
```

Full matrix:

```bash
cargo bench -p fsqlite-wal --bench raptorq_matrix -- --noplot
```

Latency lines are emitted as:

```text
INFO bead_id=bd-1eog case=decode_latency matrix=K..._T..._L... p95_ns=... p99_ns=...
```

## Default Selection Guidance

- `T=4096` remains the preferred default for page-centric WAL/DB payload paths:
  it aligns with page size and avoids extra reshaping in page I/O paths.
- `T=1366` remains appropriate for MTU-constrained replication transport.
- repair overhead default (`20%`) should be kept when:
  decode throughput and p99 latency at `10%` loss stay within acceptable SLO
  bounds for the target workload.

When to raise overhead:

- if `20%` loss scenarios show frequent decode-path tail inflation (`p99`)
  beyond latency budget.

When to lower overhead:

- only with measured evidence that decode tails and recovery success remain
  stable under expected loss distributions.

## Notes

- The benchmark is deterministic (fixed symbol generation and deterministic loss
  selection).
- Smoke mode is intentionally reduced and should be treated as a regression
  signal, not a final tuning source.

## Smoke Run Snapshot (2026-02-10)

Command:

```bash
FSQLITE_BENCH_SMOKE=1 cargo bench -p fsqlite-wal --bench raptorq_matrix -- --noplot
```

Representative results from this run:

- systematic fast path:
  - `K32,T1366,L0`: ~`5.46 GiB/s`
  - `K32,T4096,L0`: ~`8.13 GiB/s`
  - `K256,T1366,L0`: ~`5.14 GiB/s`
  - `K256,T4096,L0`: ~`7.78 GiB/s`
- repair generation:
  - `K256,T4096,L10`: ~`392.72 MiB/s`
  - `K1024,T4096,L20`: ~`156.33 MiB/s`
- decode throughput:
  - `K32,T4096,L0`: ~`107.84 MiB/s`
  - `K32,T4096,L20`: ~`74.08 MiB/s`
  - `K256,T4096,L20`: ~`45.91 MiB/s`
  - `K1024,T4096,L20`: ~`22.40 MiB/s`
- decode latency summaries (from emitted `INFO` lines):
  - `K32,T4096,L0`: `p99 ~ 1.32 ms`
  - `K32,T4096,L20`: `p99 ~ 1.87 ms`
  - `K256,T4096,L20`: `p99 ~ 23.83 ms`
  - `K1024,T4096,L20`: `p99 ~ 184.32 ms`
- hash/auth verification:
  - `verify_wal_fec_source_hash(4096B)`: ~`14.46 GiB/s`
  - `verify_symbol_auth_tag(4096B)`: ~`1.04 GiB/s`

Default justification based on the above:

- Keep `T=4096` as default for page-native paths:
  higher systematic throughput than `T=1366` in representative smoke cases,
  plus direct page alignment for WAL/DB payload flow.
- Keep baseline repair overhead at `20%`:
  decode remains successful through `20%` loss scenarios in smoke mode while
  preserving a bounded tail. Increase overhead only for deployments with
  sustained high-loss links where `p99` decode latency exceeds budget.
