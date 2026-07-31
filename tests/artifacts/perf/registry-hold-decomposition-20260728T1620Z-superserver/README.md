# Registry commit-lock hold/wait decomposition (bd-i0tn6 evidence)

First counter-enabled matrix run: RusticBasin's core shim (57dffe1a) +
WildFrog's mvcc counters (c53aaf93) + per-arm bench dump. Shared table,
8/32/64/128 writers (run clipped at the tail by the task window; the 64/128
counter lines are captured). Host superserver, DIAGNOSTIC-ONLY.

Per-round registry guard statistics (each line = one F invocation):
  64 writers:  mean hold 713-2,340us per acquisition, max 4.6ms;
               mean wait 0.2-1.1us
  128 writers: mean hold 2,436-6,949us, max single hold 60ms;
               mean wait 65-1,087us

VERDICT: the concurrent_registry guard — held across validate -> physical
page write -> publish — is held for MILLISECONDS per commit, growing
~3-10x from 64->128 writers, with cumulative hold ~0.5-0.9s per bench
round. Combined with the separate-tables split (~80% of the high-writer
decline is registry-side), this quantifies bd-i0tn6 (move physical write
outside the lock) as the primary recovery lever. Low mean WAITs at 64w
show queueing manifests as txn latency (writers busy between commits),
not lock spin — consistent with the convoy signature (p95 10x at 32w).
