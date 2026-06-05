# Track H Matched Artifact Packs

- run_id: `bd-db300.8.3.2-20260605T122244Z-3555737`
- campaign_manifest: `sample_sqlite_db_files/manifests/beads_benchmark_campaign.v1.json`
- pack_count: `4`

| row_id | fixture_id | placement_profile_id | storage_profile_id | comparability | sqlite ops/s | mvcc ops/s | single-writer ops/s | single-writer vs mvcc ops ratio | single-writer minus mvcc retries |
| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| mixed_read_write_c4 | frankensqlite | adversarial_cross_node | file_backed | declared_only_requires_external_placement_enforcement | 2485.7094389359813 | 2588.8337258760334 | 2298.4795684071364 | 0.887843643812758 | 6 |
| mixed_read_write_c4 | frankensqlite | adversarial_cross_node | memory | declared_only_requires_external_placement_enforcement | 345355.1459643524 | 38337.9795769749 | 37495.66456378481 | 0.9780292278705274 | 0 |
| mixed_read_write_c4 | frankensqlite | recommended_pinned | file_backed | declared_only_requires_external_placement_enforcement | 10984.25939152805 | 2569.2245107974936 | 2308.125684921871 | 0.8983744609401314 | 6 |
| mixed_read_write_c4 | frankensqlite | recommended_pinned | memory | declared_only_requires_external_placement_enforcement | 356346.1689223379 | 37931.31700567149 | 38829.78671768901 | 1.0236867523445912 | 0 |
