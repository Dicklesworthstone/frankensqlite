# highwriter-decomp-20260825 — registry-guard HOLD decomposition (bd-i0tn6 GATE)

Decomposes the commit-path `concurrent_registry` guard HOLD into its three
under-lock sub-phases (validate / physical-write / publish), satisfying the
bd-i0tn6 landing GATE ("land [S3] only after RegistryCommitLockMetrics decomposes
hold into validate/IO/publish").

## Provenance
- Tree: HEAD `9983798bf` + the bd-i0tn6 telemetry-only decomposition commit.
- Binary: `mt-mvcc-bench`, profile `release-perf` with **LTO off, codegen-units=16**
  (fast build). Absolute times are NOT comparable to the 2026-07-28 canonical
  (LTO on, cgu=1) superserver artifacts; ratios (io_frac) and the convoy shape are.
- Host: this box (NOT the 2026-07-28 superserver). 128 writers is OVERSUBSCRIBED
  here — F falls below C at 128w and that arm is confounded by deschedule stalls.
- Workload: shared table, 1000 rows/thread, `synchronous=normal`, 11 iters,
  threads=1,8,32,64,128.

## Files
- `matrix-normal.json` — full mt-mvcc-bench JSON (throughput + latency matrix).
- `stdout-normal.log`  — the throughput table.
- `stderr-normal.log`  — per-round `registry_lock` (total hold/wait) and
  `registry_lock_decomp` (mean_validate_us / mean_io_us / mean_publish_us /
  io_ns_max / io_frac) lines.
- `summary-normal.md`  — harness-emitted summary.

## Headline
io (physical WAL/pager write, held under the global registry mutex) is the
dominant sub-phase and its share grows with writer count: io_frac 0.99 (1w) ->
0.73 (8w) -> 0.85 (32w) -> 0.93 (64w). At 32-64w — where the convoy bites — io is
85-93% of the hold, so moving the physical write outside the guard (S3) is the
correct primary lever. Validate + publish are a non-trivial residual under
contention (~27% at 8w, ~15% at 32w); re-measure after S3 to confirm they do not
inherit the convoy. See the 2026-08-25 entry in
`docs/progress/perf-negative-results.md` for the full analysis.
