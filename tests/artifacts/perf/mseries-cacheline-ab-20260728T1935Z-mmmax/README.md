# M-series cache-line premise A/B — PROVEN on real hardware (bd-64uz9/bd-y3dlq)

## Question
Does 64-byte padding on adjacent hot atomics false-share on Apple M-series
(128-byte L1 lines), and is 128-byte padding sufficient? This was the
same-host evidence RusticBasin's audit (platform-perf 4534) required before
any VendorCachePadded call-site flips.

## Method
Standalone dependency-free microbench (cacheline_ab.rs, this dir): N threads
each fetch_add a private AtomicU64 in an adjacent-slot array, slots padded to
64 / 128 / 256 bytes; 20M ops/thread, 3 reps, `rustc -O`.

- mmmax = Mac mini M4 Pro (Darwin 25.2.0, arm64 T6041), hw.cachelinesize=128
  (primary-source premise check), 14 cores, macOS 26.2.
- superserver x86-64 control (taskset 8 quiet cores), 64B expected granule.

## Results (ns/op, rep-range)
M4 Pro (mmmax-cacheline-run2.txt + first run in session log):
- 2 threads: 64B 1.45-1.56 | 128B 0.90-0.93 | 256B 0.90-0.93  -> 64B ~1.65x slower
- 4 threads: 64B 0.80-0.83 | 128B 0.46-0.50 | 256B 0.46-0.49  -> 64B ~1.7x slower
- 8 threads: 64B 0.55-0.68 | 128B 0.25-0.32 | 256B 0.27-0.38  -> 64B ~1.9-2.1x slower
128B == 256B within noise at every arm -> coherence granule is exactly 128B.

x86 control (superserver-cacheline-run2.txt):
- 4 threads: 1.40 | 1.40 | 1.40 (identical); 8 threads: ~0.70-0.74 all arms
  (one 1.05 outlier on shared host). No 64B false-sharing effect on x86.

## Verdict
- PREMISE PROVEN: adjacent 64B-padded atomics on M-series pay 1.65-2.1x under
  cross-thread contention; 128B eliminates it entirely; no benefit beyond 128B.
- Vendor scoping of VendorCachePadded (target_vendor="apple" -> 128B) is
  exactly right: x86 shows zero effect, so 128B there would only waste cache.
- What this does NOT yet prove: product-level impact. The in-memory flip
  candidates in fsqlite-mvcc (CommitSlot commit_combiner.rs:114, seqlock
  telemetry stripes, shared_lock_table occupancy stripes) are ns-scale touches
  inside us-scale operations; the call-site flip lands only if an
  mt_mvcc_bench A/B on this mini shows a gated win.
  SharedTxnSlot / generic CacheAligned in shm remain untouchable (file format).

## Product-level A/B (mt_mvcc_bench, same mini) — SUGGESTIVE-POSITIVE, NOT GATED
Flip arm = flip128.patch (this dir): CommitSlot + both striped counters
repr(align(64)) -> repr(align(128)); repo @688d3c6d; release-perf.
- Unpaired pass (mmmax-unpaired-runs.txt): F wps 2w 77.4k->88.4k (+14.2%),
  4w 80.3k->88.6k (+10.4%), 8w 89.8k->102.2k (+13.8%) — BUT the C arm
  drifted +33% at 2w between the same two runs -> unpaired deltas untrusted.
- Interleaved A/B/A/B x3 at threads=8 (mmmax-interleave.txt): flip wins all
  three pairs, +1.3% / +7.8% / +5.1% (mean +4.7%); within-run cv 42-52%;
  sign test 3/3 = p=0.125. VERDICT: plausible small positive, NOT
  significance-gated; flip NOT landed (one-lever discipline, 4568).
- RETRY CONDITION: quiesced mini + QoS pinning (bd-y3dlq) to cut the 42-52%
  cv, >=10 interleaved pairs, or perf-c2c-equivalent HITM sampling on macOS
  (Instruments). Note the run-to-run machine drift on this host (C arm
  115k->161k wps across the session) — any future mac receipt needs
  interleaving as a hard requirement.
- Context that dwarfs this lever: in these same runs F is 0.66-0.85x C at
  8 writers on macOS (bd-jyeus checkpoint-barrier investigation).
