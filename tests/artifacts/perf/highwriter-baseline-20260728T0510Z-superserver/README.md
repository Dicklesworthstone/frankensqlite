# First 1-128 writer scaling attempt (post-revert tree a29e136d)

mt-mvcc-bench --threads=1,8,16,32,64,128 --rows-per-thread=1000 --iters=7,
shared table, synchronous=NORMAL both arms, ELF 4132d5a7..., host
superserver (64 logical CPUs, shared multi-agent box) — DIAGNOSTIC-ONLY
provenance; the within-run gates and the latency SHAPE are the evidence.

Median-CI-gated results (same-invocation C/C null + C/F claim):
  threads=1:  F/C 2.111x  FSQLITE_FASTER  (F 134.6k wps, C 65.1k)
  threads=8:  F/C 2.703x  FSQLITE_FASTER  (F 149.6k, C 54.7k)
  threads=16: F/C 6.556x  FSQLITE_FASTER  (F 123.5k, C 18.8k)
  threads=32: F/C 7.333x  INCONCLUSIVE (CV 70%; ci95 [1.21,7.47])
              (F 95.4k, C 13.0k; zero failed writes through 32)

FINDING 1 (engine signal): F p95 per-txn latency 129/225ms at 8/16 writers
-> 2,273ms at 32 (10x latency for 2x writers; C p95 2,551ms at 32).
Throughput holds (95k wps) while tail latency explodes = convoying at a
serialization point — consistent with the concurrent_registry mutex held
across physical commit (RusticBasin's audit hypothesis, msg 4470).

FINDING 2 (harness envelope): the 64-writer arm aborts — INSERT exhausted
retry budget "after 1 retries: database is busy". FSQLITE_RETRY_TIMEOUT is
a fixed 5s wall-clock per-txn budget (mt_mvcc_bench.rs:94,:454); with
64 concurrent 1000-row txns and multi-second p95s, queueing alone exceeds
it before meaningful retrying. The 64/128 arms are unmeasurable until the
budget scales with threads x rows (beaded).
