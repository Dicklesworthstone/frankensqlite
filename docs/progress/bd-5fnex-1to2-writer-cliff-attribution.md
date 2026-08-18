# bd-5fnex — Attribution of the 1→2 writer write-throughput cliff

**Date:** 2026-08-18 · **Bead:** bd-5fnex (P1) · **Verdict:** the cliff is **disk / FS-metadata
bound (a per-commit WAL-file open storm)**, **not** an in-process commit lock. The bead's two
stated hypotheses — rightmost-leaf page-lock vs commit-path `inner.lock` — are **both refuted**
by the data.

> Deliverable is the **attribution** (which mechanism). Lever selection follows the evidence at
> the end; implementation is a separate follow-up.

## Method

- Harness: `crates/fsqlite-e2e/src/bin/mt-mvcc-bench` (shared table `bench(id INTEGER PRIMARY
  KEY, payload TEXT)`, `PRAGMA fsqlite.concurrent_mode=ON` + `BEGIN CONCURRENT`,
  `concurrent_mode` asserted default-true). `--threads=1,2 --rows-per-thread=1000 --iters=11`.
- Instrumentation added to the bench (per thread-count arm, reset-before/snapshot-after):
  the registry commit-lock metrics (already present) **plus** the full group-commit
  `GLOBAL_CONSOLIDATION_METRICS` phase breakdown (`arrival_wait / consolidator_lock_wait /
  inner_lock_wait / exclusive_lock / wal_append / wal_sync / waiter_epoch_wait / flusher_commits
  / waiter_commits`) and the VDBE `page_lock_wait` counter. All three suspects are already
  instrumented in-tree — **no `perf record`** (unreliable on this shared box) was needed.
- **Gated A/B — on-disk vs tmpfs**: same binary, DB on `/tmp` (nvme, `env -u TMPDIR`) vs
  `/dev/shm` (tmpfs, `TMPDIR=/dev/shm`). tmpfs makes fsync ~free and file metadata RAM-fast, so
  it cleanly separates disk/FS-bound cost from in-process lock/CPU cost.
- Build: debug, incremental. **Attribution is a relative decomposition (which cost explodes
  1→2), which is robust in a debug build.** Absolute throughput vs C is not the subject here.
- **Measurement confound (stated up front):** the host was under heavy load during these runs
  (~19 competing `rustc`/`cargo` processes, `/` at ~89%). This *amplifies* the disk-bound cost.
  The tmpfs A/B controls for it — it isolates the *mechanism* regardless of absolute magnitude —
  but the intrinsic (quiet-host) cliff magnitude is a follow-up (needs a dedicated quiet host;
  see bd-smxhz, which found F scales ~40× at 8w on a quiet host).

## Result 1 — the cliff is real and severe on disk, and it VANISHES on tmpfs

F = FrankenSQLite p50 writes/sec; C = C-SQLite (rusqlite).

| workload | medium | F 1w | F 2w | **F 1→2** | C 1→2 |
|---|---|---|---|---|---|
| one-row-per-txn | **disk** | 1245 | 265 | **0.21× (collapse)** | 17840→27139 (grows) |
| one-row-per-txn | **tmpfs** | 1202 | 1011 | **0.84× (≈flat)** | 67345→61996 |
| bulk (default granularity) | disk | 65146 | 12634 | **0.19× (collapse)** | 125087→162172 (grows) |

- On **disk**, F throughput **collapses ~4.7×** from 1→2 writers while C *grows*.
- On **tmpfs**, the same 1→2 transition is **≈flat** (0.84×, a ~16% dip). The cliff is
  overwhelmingly **disk / FS bound**.
- At **1 writer**, disk and tmpfs give the *same* F throughput (1245 vs 1202) — so at 1 writer F
  is **not** disk-bound (it is CPU/overhead-bound); the disk cost only bites when the **second**
  writer is added and the two writers contend on the shared file system.

## Result 2 — the in-process commit locks do NOT convoy at 2 writers

Per-iter phase totals (one-row, 2001 commits/iter at 2w), disk **and** tmpfs are ≈identical:

| phase field | disk 1w | disk 2w | tmpfs 2w | per-commit @2w |
|---|---|---|---|---|
| registry `mean_wait_us` | 0.1 | **0.1–0.3** | **0.7–1.1** | ≈0 (no convoy) |
| `consolidator_lock_wait_us` | 0 | 0–19 | 0–19 | ≈0 |
| `inner_lock_wait_us` | ~50 | ~1100 | ~1800 | ~0.5–0.9 µs |
| `exclusive_lock_us` (RESERVED flock) | ~4200 | ~9500 | ~10000 | ~4.7 µs (flat) |
| `wal_append_us` | ~55700 | ~115000 | ~107000 | ~57 µs (flat) |
| `wal_sync_us` | **0** | **0** | **0** | 0 (NORMAL WAL = no per-commit fsync) |
| `page_lock_wait` | 0 | sporadic ≤24ms spikes | sporadic ≤27ms spikes | shared non-leaf page, not rightmost leaf |

Reads:
1. **Registry mutex wait ≈ 0** at 2 writers (holds are per-commit: 2001/iter). The
   `concurrent_registry` guard is **not** convoying at 1→2 — this refutes the low-writer-count
   bd-i0tn6 convoy hypothesis. (It convoys at 64+ writers per the earlier campaign, not here.)
2. **Consolidator lock wait ≈ 0**; **inner.lock wait** grows but is tiny (~0.5–0.9 µs/commit).
3. **Every measured commit-path phase is ≈flat per-commit** (`wal_append` 57 µs, `exclusive_lock`
   4.7 µs) and **identical disk-vs-tmpfs** — yet throughput differs 4× (disk 265 vs tmpfs 1011).
   **Therefore the 2-writer disk cost lives entirely *outside* the measured commit path.**
4. `wal_sync_us = 0` throughout ⇒ the cliff is **not fsync** (NORMAL sync in WAL mode fsyncs only
   at checkpoint).
5. **Rightmost-leaf page-lock is not the mechanism:** on the representative concurrent-writer
   workload each writer gets a **disjoint** rowid range (`base = tid × 1_000_000`), so the two
   writers hit **different** leaves — the shared fast-array CAS slot they occasionally block on is
   a shared **non-leaf** page (root/interior/freelist), and only sporadically (a few waits/iter).
6. **Group-commit never coalesces the two writers:** `flusher_commits=2001, waiter_commits=0` —
   every commit flushes solo, even in the OLTP one-row shape (contra the coalescing bd-acuhw
   expects). Since `wal_sync=0` here, coalescing fsync would not help, but coalescing file ops
   could.

## Result 3 — direct cause: a per-commit WAL-file open storm (strace, current HEAD)

`strace -f -e trace=openat` of 200 one-row commits, 1 writer (fsqlite DB = `.tmpDrYVJd`):

| fsqlite file | openat count | per commit |
|---|---|---|
| `-wal` | **2028** | **~10.1 opens/commit** |
| `-wal-cert` (durable certificate sidecar) | 203 | ~1.0/commit |
| main `.db` | 4 | bootstrap only (**not** per-commit) |
| `-fsqlite-ns-gate` / `-fsqlite-ns-use` | 5 / 5 | bootstrap per-connection |

FrankenSQLite reopens the **WAL file ~10× per commit** (plus the cert sidecar once). Each
`openat`/`close` is an FS-metadata syscall. On tmpfs those are RAM-fast (→ no cliff); on a disk
shared by the second writer (and, here, 19 build processes) they serialize on FS metadata and the
device. This is the **bd-smxhz per-commit file-open storm**, confirmed at HEAD — but now
concentrated on the **WAL sidecar** (bd-smxhz's earlier ~17/commit *main-DB* storm is gone at
HEAD; the main DB is opened only 4× total).

## Attribution (answering bd-5fnex)

The 1→2 writer write-throughput cliff is **not** attributable to either lock the bead named:
- **NOT** the rightmost-leaf page-lock (disjoint leaves; only sporadic shared non-leaf spikes).
- **NOT** the commit-path `inner.lock` / consolidator / registry mutex (all waits ≈0 at 2w; all
  commit-path phases flat per-commit and identical disk-vs-tmpfs).

It is the **per-commit WAL-file (and cert) open storm** — ~10 `openat`/commit on `-wal` — which is
**disk / FS-metadata bound**: it disappears on tmpfs and is invisible to every commit-path lock
counter. A minor residual (~16%, the part that persists on tmpfs at 2w) is genuine in-process
contention: the modest `inner.lock` growth plus sporadic shared non-leaf page-lock spikes.

## Lever (evidence-selected; implementation is follow-up)

1. **PRIMARY — eliminate the per-commit WAL/cert reopen storm** via VFS file-descriptor caching /
   persistent handles across a connection's commits (the bd-smxhz direction, now pinpointed to the
   `-wal` + `-wal-cert` files, ~11 opens/commit total). This directly removes the attributed cost.
2. **Secondary — close the group-commit coalescing gap** (`waiter_commits=0` at 2 writers;
   bd-6a8a5 / bd-acuhw): batching the two writers' commits could share the file operations. Bounded
   value while `wal_sync=0`.
3. **Do NOT** pursue the bead's original levers for this cliff: rightmost-leaf steal-on-same-page
   (bd-77l3t) and registry `inner.lock` tuning (bd-i0tn6) are refuted at 1→2 by this data. (They
   remain relevant to the separate 64+-writer registry convoy.)

## Follow-ups
- Quantify the **intrinsic** (quiet-host) 1→2 cliff magnitude — this run's host was heavily loaded
  (amplifies the disk cost). Needs a dedicated quiet host / lower system load.
- The bench instrumentation added here (per-arm `commit_lock_decomp` line) is retained in
  `mt_mvcc_bench.rs` for future commit-path attribution.

## Reproduce
```
# build (debug)
CARGO_TARGET_DIR=/data/tmp/cargo-target RCH_CARGO_WRAPPER_BYPASS=1 \
  cargo build -p fsqlite-e2e --bin mt-mvcc-bench
BIN=/data/tmp/cargo-target/debug/mt-mvcc-bench
# disk vs tmpfs A/B (decomp lines go to stderr)
FSQLITE_BENCH_PROFILE_INSERT=1 env -u TMPDIR      "$BIN" --one-row-per-transaction --threads=1,2 --rows-per-thread=1000 --iters=11
FSQLITE_BENCH_PROFILE_INSERT=1 env TMPDIR=/dev/shm "$BIN" --one-row-per-transaction --threads=1,2 --rows-per-thread=1000 --iters=11
# storm confirmation
strace -f -e trace=openat -o storm.txt "$BIN" --one-row-per-transaction --threads=1 --rows-per-thread=200 --iters=1
grep -c -- '-wal"' storm.txt   # ~10 x commits
```
