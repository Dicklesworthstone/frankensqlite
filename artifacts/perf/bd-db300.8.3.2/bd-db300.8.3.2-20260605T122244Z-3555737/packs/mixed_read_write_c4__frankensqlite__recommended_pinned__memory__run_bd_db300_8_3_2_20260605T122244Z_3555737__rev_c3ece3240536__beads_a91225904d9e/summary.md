# Matched Mode Pack

- row_id: `mixed_read_write_c4`
- fixture_id: `frankensqlite`
- placement_profile_id: `recommended_pinned`
- storage_profile_id: `memory`
- hardware_class_id: `linux_x86_64_many_core_numa`
- comparability_status: `declared_only_requires_external_placement_enforcement`
- source_revision: `c3ece32405362d66cae9731a80ff5a13806182eb`
- beads_data_hash: `a91225904d9ee8eb9881c285a7ab3935b4393214daa5b5c81930eab8daed09e8`

## Mode Summary

| Mode | Median ops/s | Median latency (ms) | P95 latency (ms) | Mean retries | Mean aborts |
| --- | ---: | ---: | ---: | ---: | ---: |
| sqlite_reference | 356346.1689223379 | 2.0 | 2.0 | 0 | 0 |
| fsqlite_mvcc | 37931.31700567149 | 9.0 | 9.0 | 0 | 0 |
| fsqlite_single_writer | 38829.78671768901 | 9.0 | 9.0 | 0 | 0 |

## Deltas

- mvcc_vs_sqlite_median_ops_ratio: `0.10644513766033568`
- single_writer_vs_mvcc_median_ops_ratio: `1.0236867523445912`
- single_writer_minus_mvcc_median_latency_ms: `0`
- single_writer_minus_mvcc_mean_retries: `0`

## Notes

- non-baseline placement profiles are recorded from the canonical contract but require external CPU and memory placement enforcement outside this script
- packs produced without that enforcement should be treated as declared_only rather than clean topology claims
- memory runs replay the same OpLog against :memory: connections for zero-file-placement comparison
