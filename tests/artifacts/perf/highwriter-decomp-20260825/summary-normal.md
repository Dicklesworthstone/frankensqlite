# mt-mvcc-bench Summary

- Citable: `false`
- Measurement evidence valid: `false`
- Non-citable reason: v7 binds the running executable, build/runtime source identity, Cargo.lock, invocation, toolchain, and measurement host to this same-invocation comparison, but bd-uh1fv still requires external watchdog, sanitized environment, matched retry/deadline semantics, external validation (and, when absent, capture) of a build-attested resolved dependency/feature-graph digest, counterbalanced topology receipts, immutable manifest, retained baseline history, and independent verification.
- Release regression scope: Narrow same-process, same-host F/C writer-throughput comparison for only the requested mt-mvcc-bench workload/configurations; this report does not cover the shipped release profile, other workloads or platforms, long-term baseline retention, independent reproduction, or overall release eligibility.
- Executable unchanged during measurement: `Some(true)`
- Settings interpretation: Both engines proved the listed effective PRAGMA values; equal names and readbacks do not establish cross-engine semantic equivalence.
- Accounting interpretation: offered and committed writes share one row unit; attempted_writes counts physical INSERT calls; retried_operations records the existing engine-specific retry unit and is provenance only, not a cross-engine comparison metric.
- Timing interpretation: workload_elapsed_ns begins only after every worker has opened and proved its effective settings, and ends at the last worker's transaction terminal point before connection teardown; worker_startup_elapsed_ns is reported separately.
- Workload shape: `shared_table`
- Rows per thread: `1000`
- Iterations: `11`
- Schema: `fsqlite-e2e.mt_mvcc_bench_report.v7`

- Pass-over-pass gate: `disabled_non_citable` (comparable pairs `0`, threshold `5.00%`, history `.bench-history/mt-mvcc-bench.latest.json`)

| Threads | Configuration | fsqlite p50 wps | sqlite p50 wps | F/C median | F/C median CI95 | C/C A/A CI95 | Verdict | fsqlite committed/offered | sqlite committed/offered | fsqlite failed | sqlite failed |
|---------|:--------------|-----------------:|---------------:|-----------:|----------------:|-------------:|:--------|----------------------------:|--------------------------:|---------------:|--------------:|
| 1 | supported | 240197 | 118639 | 2.686x | [1.335, 5.664] | [1.136, 2.523] | INCONCLUSIVE | 11000/11000 | 11000/11000 | 0 | 0 |
| 8 | supported | 115897 | 44377 | 2.748x | [1.692, 3.414] | [0.998, 1.386] | INCONCLUSIVE | 88000/88000 | 88000/88000 | 0 | 0 |
| 32 | supported | 47319 | 12629 | 3.712x | [2.484, 4.397] | [0.961, 1.043] | FSQLITE_FASTER | 352000/352000 | 352000/352000 | 0 | 0 |
| 64 | supported | 22381 | 12639 | 1.764x | [1.527, 1.932] | [0.991, 1.003] | FSQLITE_FASTER | 704000/704000 | 704000/704000 | 0 | 0 |
| 128 | oversubscribed | 9252 | 12675 | 0.734x | [0.641, 0.749] | [0.971, 1.163] | INVALID_OVERSUBSCRIBED | 1408000/1408000 | 1408000/1408000 | 0 | 0 |
