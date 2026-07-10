# Packed same-leaf DELETE atlas rejection

The one-binary release-perf gate cleanly rejected the packed atlas at all
three required stream sizes. ORIG, candidate, and C SQLite used separate
warmed fixtures in one Criterion group and rotated through all six execution
orders. Each timed window was exactly `BEGIN -> prepared sparse DELETEs ->
COMMIT`; restore INSERTs ran immediately afterward but outside the returned
duration.

| deletes | ORIG median | packed median | C median | packed / ORIG | ORIG / C | packed / C | CV ORIG / packed / C |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 64 | 38.686 us | 42.368 us | 19.994 us | 1.095158x | 1.934860x | 2.118977x | 1.360% / 0.832% / 1.015% |
| 256 | 145.357 us | 160.384 us | 76.073 us | 1.103381x | 1.910751x | 2.108285x | 0.465% / 0.526% / 0.540% |
| 1024 | 567.864 us | 627.788 us | 298.535 us | 1.105525x | 1.902168x | 2.102893x | 2.396% / 2.230% / 4.445% |

The `n=50` per-arm distribution comprises the final 50 interleaved callback
batches retained by the custom runner. The profile-slowed evidence run had
fewer callback batches and intentionally tripped the runner's assertion after
Criterion completed collection; its perf data finalized normally and is used
only for reachability/self-time, never for the timing verdict.

Self-time verification used a symbolized, unstripped release-perf build on the
same worker. Binary SHA-256:
`736c1c88890bfb63d018b4a2f56ef4244c33c216a9e1852353e977e2bc9e7003`.
The 64-delete execution recorded:

- shared `TableLeafDeleteRun::materialize_deletions`: 3.84% / 959 samples;
- candidate `PackedTableLeafDeleteAtlas::try_from_entry`: 1.93% / 481;
- candidate `try_materialize` sort: 1.79% / 447;
- legacy `TableLeafPayloadPatchRun::table_leaf_rowid_at`: 0.65% / 162.

The complete >=0.01% symbol table is retained here. Raw perf data remains at
`fmd:/data/tmp/cod-fsq-packed-atlas-reject-20260710T1620Z/perf-sudo-bench.data`
(210,753,600 bytes, SHA-256
`fa065e84386e28d27f5751297cdad1512a55f428c64935dc50a11cf6b8c5f4f4`).

Correctness proof before timing:

- packed/legacy/corrupt-atlas byte-image differential, cancellation, and
  fallback test: 1 passed;
- prepared-direct-DELETE pager-routing tests: 6 passed;
- duplicate/absent/savepoint C-SQLite conformance tests: 3 passed.
