# Matched Mode Pack

- row_id: `mixed_read_write_c4`
- fixture_id: `frankensqlite`
- placement_profile_id: `adversarial_cross_node`
- storage_profile_id: `memory`
- hardware_class_id: `linux_x86_64_many_core_numa`
- comparability_status: `declared_only_requires_external_placement_enforcement`
- source_revision: `c3ece32405362d66cae9731a80ff5a13806182eb`
- beads_data_hash: `a91225904d9ee8eb9881c285a7ab3935b4393214daa5b5c81930eab8daed09e8`

## Mode Summary

| Mode | Median ops/s | Median latency (ms) | P95 latency (ms) | Mean retries | Mean aborts |
| --- | ---: | ---: | ---: | ---: | ---: |
| sqlite_reference | 345355.1459643524 | 2.0 | 2.0 | 0 | 0 |
| fsqlite_mvcc | 38337.9795769749 | 9.0 | 9.0 | 0 | 0 |
| fsqlite_single_writer | 37495.66456378481 | 9.0 | 9.0 | 0 | 0 |

## Deltas

- mvcc_vs_sqlite_median_ops_ratio: `0.11101030352370123`
- single_writer_vs_mvcc_median_ops_ratio: `0.9780292278705274`
- single_writer_minus_mvcc_median_latency_ms: `0`
- single_writer_minus_mvcc_mean_retries: `0`

## Notes

- non-baseline placement profiles are recorded from the canonical contract but require external CPU and memory placement enforcement outside this script
- packs produced without that enforcement should be treated as declared_only rather than clean topology claims
- memory runs replay the same OpLog against :memory: connections for zero-file-placement comparison
