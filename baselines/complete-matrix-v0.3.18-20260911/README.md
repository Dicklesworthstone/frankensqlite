# Historical complete-matrix baseline

This is the immutable input to
`crates/fsqlite-e2e/tests/complete_benchmark_matrix.rs` (bd-wwqen.8).
The engine is published v0.3.18, revision
`1600766ca698dae99b6018474bc8c150ece4a82d`. It was measured on September 11,
2026 with the benchmark-only repair in `harness.patch`. This is not a
measurement of the unmodified published benchmark executable.

The repair records the actual Cargo profile, retains nanosecond engine
durations, and retries transient rollback cleanup within the existing batch
retry budget. The same repair is used for the candidate. All 16 affected
harness files had identical base bytes at v0.3.18 and candidate ancestor
`8e5979ffedadbde6f70af41b7f35cec8ed9a1e20`; no engine source was changed in
the historical control. The original control was rejected because rollback
cleanup failed in c8/c16 cells and integer milliseconds erased submillisecond
latency. Its failed samples were not edited or included in this baseline.

The retained run contains all 108 canonical cells: three pinned databases,
12 workload/concurrency rows, and three engine modes. Each cell uses three
warmups, at least 20 measured iterations, and the default ten-second
measurement window. There are 11,558 measured samples, zero failed samples,
no missing or duplicate cells, and one shared captured environment.
The separate correctness keepers remain required: these performance samples
do not establish database integrity or release acceptance.

## Provenance

- Worker: `ovh-a`, Linux 7.0.0-30-generic, AMD Ryzen 7 5800X, 16 logical CPUs,
  67,303,723,008 bytes RAM. Scheduler-default placement; no host CPU tuning.
- Rust: `rustc 1.100.0-nightly (908501772 2026-08-30)`; `release-perf`, opt-level
  3, debug assertions off, panic abort. No concurrent build was run on this
  worker during measurement. Existing host services were left unchanged.
- Build: strict RCH `30016197441355917`, exit 0 at
  `2026-09-11T21:53:00.862889Z`, 181,943 ms remote execution.
- Measurement: strict RCH `30016197441355919`, exit 0 at
  `2026-09-11T22:31:24.743719Z`, 2,200,973 ms remote execution.
- Executable SHA-256:
  `40bb544af21fd5a8f930866061e02d51aa249b57824586b1035a4ed084e0ac70`.
- `results.jsonl` SHA-256:
  `e595a08f9550e1b66a6f8eb1d19108df93409a6a05078d958b15bb2b6f56ebf9`.
- `harness.patch` SHA-256:
  `3d1d54d2736355fdacf2347ff3ac1a3f5358825cea446e3dcdef90c0e14553cf`.
- `source.sha256` SHA-256:
  `602ade87b22fedbb3bd24d2b39e97bb9a17833720466e722bcfb012bebf4f294`;
  all 1,613 listed inputs matched before and after execution.
- Build transcript SHA-256:
  `73bc36ba5ea020d1012759ce57e09b92cb0310e0fa2845aedbcba4bd04201001`.
- Measurement transcript SHA-256:
  `11777b0cd3ecdd3984464b5bfd75eb69f1317de3b662bdad7ed71dfdae8902d4`.

The command was the RCH-built `realdb-e2e` executable with:

```text
evidence-pack --output-dir artifacts/perf/release-baseline-0318-repaired-harness-20260911
```

Raw rows retain canonical fixture hashes, policies and per-iteration data.
The release gate pins the complete file hash, validates its population and
measurements, and requires matching hardware/compiler/fixture/policy metadata
before comparing a candidate. The gate's original thresholds and required
commit-keyed c1/persistent scorecards remain in force.

This retained test input belongs under `baselines/`. The generated-output tree
`tests/artifacts/` is excluded from normal RCH source synchronization.

Retire this baseline from active selection only when a reviewed, controlled
successor baseline replaces it. Preserve these historical bytes in Git.
