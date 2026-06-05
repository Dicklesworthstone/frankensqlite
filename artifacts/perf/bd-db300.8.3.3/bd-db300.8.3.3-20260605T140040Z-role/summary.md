# H3.3 Single-Writer Role Package

- bead_id: `bd-db300.8.3.3`
- run_id: `bd-db300.8.3.3-20260605T140040Z-role`
- role: `comparison_or_fallback_only`
- product default: `fsqlite_mvcc`

This package turns the H3.1 and H3.2 Track H evidence into report-ready inputs for G4. Forced single-writer mode is comparison/fallback evidence only; it is not the product default and must not be used to justify writer serialization.

Artifacts:

- `single_writer_role.json`
- `single_writer_role.md`

Source evidence:

- H1.2 baseline report: `artifacts/perf/bd-db300.8.1.2/bd-db300.8.1.2-20260415T090249Z-1463983/report.json`
- H3.1 WAL role artifact: `artifacts/perf/bd-db300.8.3.1/bd-db300.8.3.1-20260523T023545Z-969361/single_writer_role.json`
- H3.2 execution-engine report: `artifacts/perf/bd-db300.8.3.2/bd-db300.8.3.2-20260605T122244Z-3555737/report.json`
