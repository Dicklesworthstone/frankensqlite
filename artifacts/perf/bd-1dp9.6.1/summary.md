# Perf Baseline Pack Runner

- bead_id: `bd-1dp9.6.1`
- run_id: `bd-1dp9.6.1-1770975442851`
- root_seed: `13200456914808340481`
- scenario_count: `7`
- promoted_count: `6`
- baseline_path: `/data/projects/frankensqlite/baselines/criterion/bd-1dp9.6.1-baseline.json`
- smoke_report_path: `/data/projects/frankensqlite/baselines/smoke/bd-1dp9.6.1-smoke-report.json`
- hyperfine_path: `/data/projects/frankensqlite/baselines/hyperfine/bd-1dp9.6.1-hyperfine.json`
- profiling_report_path: `/data/projects/frankensqlite/artifacts/perf/bd-1dp9.6.1/profiling_artifact_report.json`
- opportunity_matrix_path: `/data/projects/frankensqlite/artifacts/perf/bd-1dp9.6.1/opportunity_matrix.json`
- overall_pass: `true`

## Promoted Opportunities
- `checkpoint::bm-checkpoint-plan-micro`
- `write-contention::bm-mvcc-page-conflict-micro`
- `recovery::bm-recovery-crash-replay-macro`
- `sql-operator-mix::bm-sql-operator-mix-macro`
- `recovery::bm-wal-checksum-recovery-micro`
- `write-contention::bm-write-contention-macro`
