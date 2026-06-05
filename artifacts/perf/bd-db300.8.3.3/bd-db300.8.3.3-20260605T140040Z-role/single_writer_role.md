# H3.3 Single-Writer Role And Report Inputs

- run_id: `bd-db300.8.3.3-20260605T140040Z-role`
- role: `comparison_or_fallback_only`
- product default: `fsqlite_mvcc`
- primary JSON: `artifacts/perf/bd-db300.8.3.3/bd-db300.8.3.3-20260605T140040Z-role/single_writer_role.json`

## Role

BEGIN stays MVCC-by-default through `concurrent_mode_default=true`. Forced single-writer is opt-in via `PRAGMA fsqlite.concurrent_mode=OFF` or `realdb-e2e --no-mvcc`.

G4 should use forced single-writer only as a causal bridge and fallback diagnostic: SQLite reference measures the external baseline, forced single-writer measures FrankenSQLite shared fixed tax without MVCC concurrency, and MVCC measures the intended concurrent-writer product path.

Do not present forced single-writer as the headline product mode, a replacement for MVCC, or evidence that FrankenSQLite should serialize writers by default.

## Source Evidence

| source | artifact | use |
| --- | --- | --- |
| H1.2 baseline | `artifacts/perf/bd-db300.8.1.2/bd-db300.8.1.2-20260415T090249Z-1463983/report.json` | baseline Track H shared-placement single-writer comparison rows |
| H3.1 WAL rerun | `artifacts/perf/bd-db300.8.3.1/bd-db300.8.3.1-20260523T023545Z-969361/single_writer_role.json` | existing role artifact plus forced single-writer verify-suite package |
| H3.2 execution-engine rerun | `artifacts/perf/bd-db300.8.3.2/bd-db300.8.3.2-20260605T122244Z-3555737/report.json` | current same-run SQLite/MVCC/single-writer comparison rows |

## H3.2 Same-Run Comparisons

| placement_profile_id | storage_profile_id | sqlite ops/s | mvcc ops/s | single-writer ops/s | single/sqlite ratio | single/mvcc ratio | retry delta vs MVCC | role read |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| recommended_pinned | file_backed | 10984.25939152805 | 2569.2245107974936 | 2308.125684921871 | 0.2101302966954772 | 0.8983744609401314 | 6 | single-writer remains below MVCC on file-backed storage but is useful fixed-tax evidence |
| recommended_pinned | memory | 356346.1689223379 | 37931.31700567149 | 38829.78671768901 | 0.10896647727438197 | 1.0236867523445912 | 0 | single-writer is slightly above MVCC in the memory fixed-tax row |
| adversarial_cross_node | file_backed | 2485.7094389359813 | 2588.8337258760334 | 2298.4795684071364 | 0.9246774914251484 | 0.887843643812758 | 6 | single-writer remains below MVCC but tracks external SQLite under adversarial placement |
| adversarial_cross_node | memory | 345355.1459643524 | 38337.9795769749 | 37495.66456378481 | 0.1085713214409584 | 0.9780292278705274 | 0 | single-writer is near MVCC in the memory fixed-tax row |

## H3.2 Single-Writer Versus H1.2 Baseline

| placement_profile_id | storage_profile_id | H1.2 single-writer ops/s | H3.2 single-writer ops/s | percent delta |
| --- | --- | ---: | ---: | ---: |
| recommended_pinned | file_backed | 1768.9725087697254 | 2308.125684921871 | 30.478324195502214 |
| recommended_pinned | memory | 34202.90806805558 | 38829.78671768901 | 13.527734660535446 |
| adversarial_cross_node | file_backed | 1073.3324965275413 | 2298.4795684071364 | 114.14422612221342 |
| adversarial_cross_node | memory | 36519.915040069645 | 37495.66456378481 | 2.6718285698216295 |

## Caveats

- The H3 matched packs use comparability `declared_only_requires_external_placement_enforcement`; do not treat `recommended_pinned` or `adversarial_cross_node` as host-enforced placement proof without a later enforcement artifact.
- Keep `file_backed` and `memory` rows separate. File-backed rows include persistence and storage fixed costs; memory rows isolate more of the in-process execution cost.
- H3.2 used `WARMUP=0` and `REPEAT=1`; use it as report input and directional evidence, not a final distributional performance claim.
- H3.1 and H3.2 were captured on different commits and dates. Same-run SQLite/MVCC/single-writer ratios are valid report inputs; cross-run movement is not causal proof without a paired rerun.

## G4 Inputs

- `role_statement.role_id`
- `role_statement.default_contract`
- `role_statement.report_contract`
- `placement_profile_caveats`
- `h3_2_same_run_comparisons`
- `h3_2_vs_h1_2_baseline_single_writer`
- `source_evidence`
