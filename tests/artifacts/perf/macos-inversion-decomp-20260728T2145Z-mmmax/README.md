# macOS write-path inversion decomposed (bd-jyeus) — mmmax M4 Pro, 2026-07-28

## Headline
On the M4 Pro mini, FrankenSQLite runs 0.24-0.85x C SQLite at 2-8 shared-table
writers (Linux: 2.1-6.6x FASTER), flat ~77-114k wps regardless of writer
count. This artifact decomposes the ceiling with direct measurements and
eliminates two plausible-but-wrong explanations.

## Eliminated (measured, not argued)
1. F_FULLFSYNC/checkpoint barriers: sudo fs_usage across 3 full paired rounds
   at 8w counted ZERO F_FULLFSYNC and only 112 fsync-family lines for BOTH
   engines combined. synchronous=NORMAL is honored (apply_synchronous_to_pager
   -> WalCommitSyncPolicy::Deferred); auto-checkpoint barriers are not the
   story either. (First hypothesis on the bead — struck.)
2. Blocking-pool saturation: pool threads are ~0.1% of thread-time (mostly
   parked); raw I/O leaves are tiny (pwrite 321 / pread 206 samples).

## Measured decomposition (sample(1) at 1ms, symbolized line-tables build,
--threads=8; jyeus-sample-sym.txt)
Representative per-round F worker thread (88 samples ~= 88ms):
- ~30% blocked in pthread_cond_wait under run_fsqlite's block_on — waiting
  behind commit serialization.
- ~28% real engine work (execute_prepared_direct_simple_insert -> BtCursor
  insert path).
- ~23% std::thread::sleep — the bench's escalating conflict-retry backoff
  (RETRY_SLEEP_MS=1 doubling to 25ms cap + jitter): workers are aborting on
  BusySnapshot/FCW conflicts and sleeping between retries.
- Remainder: thread setup/misc.

## The tie-together: registry guard telemetry (works on macOS)
registry_lock threads=8 holds=9 mean_hold_us=1419.6 hold_ns_max=4461542
(bench-context.txt): ~9 commit-guard holds per round x ~1.4ms each ~= 12.8ms
serialized per ~80ms round. At 8 writers the M4 already shows the hold times
Linux only reaches at 64 writers (713-2340us). Long holds -> queued commits ->
FCW/BusySnapshot conflicts -> whole-txn abort + backoff sleep -> flat wps.

## Verdict
Same root cause as the Linux high-writer decline — the concurrent_registry
guard held across validate+physical-write (bd-i0tn6) — but amplified ~50x at
low writer counts by Darwin: the guard section's pager writes go through
spawn_blocking round-trips whose thread handoff is costlier on macOS, and the
guard holder is subject to E-core scheduling (bd-y3dlq, unpinned QoS).
FIX PATH: (1) bd-i0tn6 S3 (write outside the guard) is THE macOS lever —
expected to collapse both the condvar-wait and the backoff-sleep components;
(2) bd-y3dlq QoS pinning of the commit-critical worker; (3) re-run this
decomposition after each.

## Provenance
- Repo @79f92a72 (+local Cargo.lock drift from mac dep resolution — noted),
  release-perf; symbolized build CARGO_PROFILE_RELEASE_PERF_STRIP=none,
  DEBUG=line-tables-only, separate target dir.
- Shared-host caveat: mini is noisy (null C/C cv 20-52%); decomposition uses
  within-thread sample fractions, which are robust to that drift.
- Files: jyeus-sample-sym.txt (9MB full call tree), bench-context.txt
  (registry_lock lines + final gates).
