# Matched Mode Pack

- row_id: `mixed_read_write_c4`
- fixture_id: `frankensqlite`
- placement_profile_id: `recommended_pinned`
- storage_profile_id: `file_backed`
- hardware_class_id: `linux_x86_64_many_core_numa`
- comparability_status: `declared_only_requires_external_placement_enforcement`
- source_revision: `c3ece32405362d66cae9731a80ff5a13806182eb`
- beads_data_hash: `a91225904d9ee8eb9881c285a7ab3935b4393214daa5b5c81930eab8daed09e8`

## Mode Summary

| Mode | Median ops/s | Median latency (ms) | P95 latency (ms) | Mean retries | Mean aborts |
| --- | ---: | ---: | ---: | ---: | ---: |
| sqlite_reference | 10984.25939152805 | 51.0 | 51.0 | 6 | 6 |
| fsqlite_mvcc | 2569.2245107974936 | 162.0 | 162.0 | 6 | 6 |
| fsqlite_single_writer | 2308.125684921871 | 157.0 | 157.0 | 12 | 12 |

## Deltas

- mvcc_vs_sqlite_median_ops_ratio: `0.23390056800543943`
- single_writer_vs_mvcc_median_ops_ratio: `0.8983744609401314`
- single_writer_minus_mvcc_median_latency_ms: `-5`
- single_writer_minus_mvcc_mean_retries: `6`

## Notes

- non-baseline placement profiles are recorded from the canonical contract but require external CPU and memory placement enforcement outside this script
- packs produced without that enforcement should be treated as declared_only rather than clean topology claims
- file_backed runs replay each iteration against a copied on-disk working database
