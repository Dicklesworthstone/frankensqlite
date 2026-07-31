# Separate-tables split experiment: registry dominates the high-writer decline (~80/20)

mt-mvcc-bench --separate-tables at 8/32/64/128 writers (7 rounds, envelope-
scaled, host superserver, DIAGNOSTIC-ONLY). All arms FSQLITE_FASTER
CI-gated, zero failed writes; 32w hits 12.37x over C.

  writers   shared-F wps   separate-F wps   recovery
  8         149.6k         202.1k           +35%
  32         95.4k         147.3k           +54%
  64         53.0k          71.5k           +35%
  128        51.8k         106.2k           +105% (CV 35%)

VERDICT: with page conflicts REMOVED (separate tables), throughput still
declines 202k->71k from 8->64 writers — the residual is the global
concurrent_registry serialization (validate->physical write->publish under
one guard). Decomposition at 64w: ~80% of the decline from the 8w peak is
registry/convoy (202->71), ~20% is shared-table leaf conflicts (71->53,
the page-45 stride-boundary class). Primary lever: bd-i0tn6
(write-outside-lock). Secondary: bd-3d5y3 (statement rebase).
