# write_single exact-gate profile calibration

- Date: 2026-07-10
- Worker: `vmi1293453` (`AMD EPYC Processor (with IBPB)`)
- Target CPU: 2; perf collector CPUs: 0-1,3-7
- Source file SHA-256: `c25b49f665c11de5db6ae4e94c0c3576934c927c80e35215424637aab4cf6e2d`
- Frozen release-perf binary SHA-256: `eba047ef0e225081b9da83f674d8ff8aab1dcccaeff179b1ca83e3008f2f80d5`
- Remote raw data: `/data/tmp/cod-fsq-write-single-exact-gate-20260710T1416Z/fs-64/perf.data`
- Raw data SHA-256: `95a7aa00e44eaeff289dd1803c9c050f97b3701d73a4238d6d8ac46f9faf49c7`

This was a measurement-substrate calibration, not an optimization A/B. The
profiled interval was acknowledged perf enable immediately before prepared
`BEGIN -> 64 sparse DELETEs -> COMMIT`, with population, three warmups, and
restore INSERTs outside the enabled interval. The planned 400,000 iterations
were stopped after the capture had already reached 451,000 cycle samples and
7,510,844,800 bytes. The resulting report has zero lost samples. The nonzero
exit status records that intentional sample-budget stop; the raw data is valid
and readable, but no wall-time summary was emitted.

The complete self-time table at or above 0.1% is
`fs-64/ranked-self-symbol-ge-0.1pct.txt`. Its leading frames were:

| frame | self-time | samples |
|---|---:|---:|
| `TableLeafDeleteRun::materialize_deletions` | 8.41% | 40,028 |
| `cell_on_page_size_fast` | 4.67% | 22,443 |
| `TableLeafDeleteRun::delete_rowid_with_reason` | 3.00% | 14,190 |
| `TableLeafPayloadPatchRun::table_leaf_rowid_at` | 2.39% | 11,315 |
| `malloc` | 1.87% | 8,214 |
| `Connection::execute_prepared_direct_simple_delete` | 1.62% | 7,396 |
| `TransactionKind::pending_conflict_pages_conservative` | 0.18% | 795 |

An unrestricted symbol report found zero samples in VDBE execution,
population/restore direct INSERT, rollback, connection-open, and
`SharedMvccState::new` frames. The next matrix should use one tenth of the
original iteration counts so every required size and both engines finish well
inside the timeout while still yielding tens of thousands of samples.
