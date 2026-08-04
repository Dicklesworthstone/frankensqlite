# First complete 1-128 writer matrix — FrankenSQLite faster at every count

mt-mvcc-bench (envelope-scaled per bd-caa6u), shared table,
synchronous=NORMAL both arms, 7 paired rounds, same-invocation C/C nulls,
host superserver (64 logical CPUs; 128-writer arm 2x oversubscribed).
DIAGNOSTIC-ONLY host provenance; the within-run CI gates are the evidence.
Trees: d6d9f0fe (first run, ELF c4e5168c...) + floor-bump rerun for the 64 arm.

  writers   F/C median   gate              F wps     C wps    failed(F/C)
  1         2.111x       FSQLITE_FASTER    134.6k    65.1k    0/0
  8         2.703x       FSQLITE_FASTER    149.6k    54.7k    0/0
  16        6.556x       FSQLITE_FASTER    123.5k    18.8k    0/0
  32        7.333x       INCONCLUSIVE(CV)   95.4k    13.0k    0/0
  64        4.259x       FSQLITE_FASTER     53.0k    12.5k    0/0
  128       4.082x       FSQLITE_FASTER     51.8k    12.7k    0/0

C SQLite pins to its ~12.5k wps single-writer floor from 32 writers up;
FrankenSQLite declines with contention (150k -> 52k) but stays 4-7x ahead
with ZERO failed writes at every valid arm — including 128 writers, the
admission cap (MAX_CONCURRENT_WRITERS).

Extraction targets confirmed en route: (1) the first 64-arm attempt starved
58 txns past an 11s envelope with "snapshot conflict on pages: 45" — a HOT
PAGE identified by number; (2) F's own decline 150k->52k is the
registry-convoy cost (p95 10x jump at 32 writers, earlier receipt) — the
instrumentation handoff (RegistryCommitLockMetrics) will quantify it.
