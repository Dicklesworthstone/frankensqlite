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
- Iterations: `21`
- Schema: `fsqlite-e2e.mt_mvcc_bench_report.v7`

- Pass-over-pass gate: `disabled_non_citable` (comparable pairs `0`, threshold `5.00%`, history `/data/projects/frankensqlite/tests/artifacts/perf/mt-mvcc-shared-table-restore-20260817/mtmvcc_release.history.json`)

| Threads | Configuration | fsqlite p50 wps | sqlite p50 wps | F/C median | F/C median CI95 | C/C A/A CI95 | Verdict | fsqlite committed/offered | sqlite committed/offered | fsqlite failed | sqlite failed |
|---------|:--------------|-----------------:|---------------:|-----------:|----------------:|-------------:|:--------|----------------------------:|--------------------------:|---------------:|--------------:|
| 1 | supported | 177406 | 179079 | 1.013x | [0.926, 1.666] | [0.848, 1.219] | INCONCLUSIVE | 21000/21000 | 21000/21000 | 0 | 0 |
| 2 | supported | 127304 | 206782 | 0.629x | [0.553, 0.814] | [0.999, 1.044] | FSQLITE_SLOWER | 42000/42000 | 42000/42000 | 0 | 0 |
| 4 | supported | 92993 | 114858 | 0.885x | [0.781, 0.934] | [0.999, 1.002] | FSQLITE_SLOWER | 84000/84000 | 84000/84000 | 0 | 0 |
| 8 | supported | 83029 | 61433 | 1.348x | [1.208, 1.485] | [0.998, 1.002] | FSQLITE_FASTER | 168000/168000 | 168000/168000 | 0 | 0 |
