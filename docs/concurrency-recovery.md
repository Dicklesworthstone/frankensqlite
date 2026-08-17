# Concurrency Recovery Reference (issue #70)

Developer-facing map of every corruption / stale-read failure mode behind
[GH #70](https://github.com/Dicklesworthstone/frankensqlite/issues/70) —
"multi-process concurrent access consistently corrupts or stales the database
under swarm write load" — with, for each mode: the production code that admitted
it, the fix that closed it, and the regression test that locks it in.

This is the **recovery** companion to [`concurrency-contract.md`](./concurrency-contract.md),
which defines *what concurrency is supported and what guarantees hold*. Read the
contract first for the model; read this for "a DB went bad — what was it, is it
fixed, and how do I recover?"

Tracked by `bd-zywqc.8` under the `bd-zywqc` epic ("Multi-process concurrent
durability — close #70 with stock-SQLite-verifiable correctness").

---

## 1. The symptom and the standard

#70's realistic workload is a swarm of AI agents coordinating through one shared
`beads_rust` database (fsqlite is its storage backend). Under concurrent
multi-process writes the file exhibited: malformed pages, missing writes, stale
reads, and wrong-row returns.

**The bar for "fixed" is stock parity, not "fsqlite says ok":** every repaired or
suspected image must read `PRAGMA integrity_check == ok` under **C SQLite**, and
every divergence between fsqlite's checker and stock is itself a bug (see §5).

---

## 2. How a corruption is classified — the fingerprint (`bd-2bjaf`)

Before fixing modes we made them *addressable*. `bd-2bjaf` built a normalizer that
turns a raw `integrity_check` failure dump into a stable signature so identical
corruption groups together.

- Code: `crates/fsqlite-e2e/src/corruption_fingerprint.rs` — `fingerprint()`,
  `fingerprint_collection()`, `normalize()`.
- Signatures (`FailureKind`): `PageNeverUsed` (`"page_never_used"`),
  `PageDoubleReference`, `MultipleUsesForByte`, `OffsetOutOfRange`, `ExtendsOffEnd`,
  `RowidOutOfOrder`, `FreeSpaceCorruption`, `ChildPageDepthDiffers`,
  `FreelistLeafCountTooBig`, `InvalidPageNumber`, `PtrmapReadFailed`,
  `PageReadFailed`, `PagePointerMapReferenced`.
- Test: `crates/fsqlite-e2e/tests/bd_2bjaf_corruption_fingerprint.rs`.

Each distinct signature was filed as a sub-bead under `bd-zywqc` with a minimal
repro, the suspected code path, and a per-OS occurrence rate.

---

## 3. Failure-mode catalog

| # | Mode (signature) | Admitting code | Fix | Regression test |
|---|---|---|---|---|
| A | Torn / lost commit across a crash | WAL publish ordering | `bd-qkeae` two-phase commit | `recovery_crash_wal_replay.rs`, `bd_3a7d_crash_recovery_wal_integrity.rs` |
| B | Stale reads / stale plans after another process's DDL | prepared-stmt cache not epoch-bound | `bd-misnw` schema-epoch binding | schema-epoch reject tests (connection.rs) |
| C | Orphaned pages / freelist holes (`page_never_used`, leaks) | pager freelist accounting under WAL churn | `bd-84rh4`, `bd-ioq6x`/GH#346 | `bd_84rh4_freelist_hole_repair.rs`, `bd_ioq6x_churn_orphan_leak_repro.rs` |
| D | All-zero page 1 on reload | in-memory reload of a committed empty image | `3fa681efb` | empty-schema-reload keeper |
| E | `integrity_check` **false**-positive vs stock | checker counted stock-legal trailing slack | `bd-2whq5` | `d4b15cdd3` oracle, `71c08c545` bit-flip |
| F | Silent corruption on upgrade from a pre-fix binary | old code kept writing latent corruption | `bd-zywqc.5` first-open repair | `migration_first_open_repair.rs` |
| G | Downgrade opens a newer on-disk format | no format gate | `bd-yaomh.6` version handshake | `b65a5f7b4` |
| H | Stale locks left by a crashed writer | no cross-process liveness check | `bd-lyrja` (Linux), `bd-4dr7g` (macOS/Win) | `bd-lyrja` liveness matrix (`b9d7a7989`) |

Detail per category below.

### A. Durability & commit ordering — `bd-qkeae` (closed)

The invariant: a reader that observes `CommitIndex.publish_seq == N` must always
find WAL frame `N` durable on disk. `bd-qkeae` enforces the strict order:
(1) write WAL frames (header + body), (2) `fdatasync` the WAL fd with
platform-correct semantics, (3) a Release memory barrier, (4) publish to the
`CommitIndex` with a Release atomic. Without the fsync-before-publish barrier a
crash between the WAL write and the publish could expose a `publish_seq` whose
frame was not yet durable → torn/lost commit on replay.

- Code: `crates/fsqlite-wal/src/wal.rs` (publish ordering), fault points in
  `crates/fsqlite-wal/src/fault_hooks.rs` ("after fsync completes but before
  publish").
- Tests: `crates/fsqlite-e2e/tests/recovery_crash_wal_replay.rs`,
  `crates/fsqlite-harness/tests/bd_3a7d_crash_recovery_wal_integrity.rs`,
  `crates/fsqlite-wal/tests/bd_26631_3_crash_loop_replay.rs`.

### B. Stale reads / stale plans — `bd-misnw` (closed)

#70 success-criterion #4: a schema-stable reader must not see stale plans after a
DDL commit in **another** process. Each `PreparedStatement` is bound to the
`SchemaEpoch` it was planned under; `step()` is rejected with
`FrankenError::SchemaChanged` (→ `ErrorCode::Schema`, i.e. `SQLITE_SCHEMA`) if a
newer epoch has been committed — matching C SQLite's re-prepare semantics.

- Code: `SchemaEpoch` binding threaded through `crates/fsqlite-core/src/connection.rs`;
  error variant `FrankenError::SchemaChanged` at `crates/fsqlite-error/src/lib.rs:85`
  (code mapping at `:471`).
- Also covers prepared-stmt cache invalidation and SAVEPOINT correctness under the
  same epoch discipline.

### C. Page / freelist corruption — `bd-84rh4`, `bd-ioq6x` (GH#346)

Two related pager defects produced `page_never_used` orphans and freelist holes
under WAL churn:

- **`bd-84rh4` — freelist-hole orphans:** abandoned-EOF allocation holes were not
  reconciled to the durable freelist. Fix: reachability repair
  (`905a7515f fix(core): reachability repair for bd-84rh4 freelist-hole orphans`)
  + fold abandoned-EOF pages at checkpoint. Reproducer: `e41984f6e`;
  test `crates/fsqlite-core/tests/bd_84rh4_freelist_hole_repair.rs`.
- **`bd-ioq6x` / GH#346 — WAL-churn orphan-page leak:** pages abandoned during WAL
  churn were dropped at checkpoint clear / `refresh_committed_state` without being
  returned to the freelist. Fix: re-park and fold abandoned EOF pages
  (`ca8ddd95f fix(pager): re-park and fold abandoned EOF pages so WAL churn cannot
  orphan them`). Repro: `1f25fa9a0`; test
  `crates/fsqlite-e2e/tests/bd_ioq6x_churn_orphan_leak_repro.rs`.

### D. All-zero page 1 on reload — `3fa681efb`

A committed empty image could reload with an all-zero page 1 (no valid header).
Fix: `3fa681efb fix(core): repair committed all-zero page 1 to a valid empty image
on in-memory reload (empty-schema-reload)`.

### E. The integrity checker itself — `bd-2whq5` (closed)

The checker is part of the trusted base; a **false** positive is as harmful as a
missed defect because downstream fail-closes on the verdict (GH#214) and would
re-trigger reconstruction in a loop. `bd-2whq5`: a stock-legal image (`beads_rust`
db, `integrity_check == ok` under C SQLite) was flagged `page N is never used`.
Root cause: fsqlite's orphan-scan/repair counted pages in the file's trailing
slack, which stock ignores past the header page count.

- Fix (`cfb8a3bc4 fix(core): bound integrity_check and first-open repair by header
  page_count`): clamp the scanned total to `header.page_count.min(published_db_size)`
  when `!header.is_page_count_stale()` — see
  `crates/fsqlite-core/src/connection.rs:62440` and `:62516`.
- Oracle test `d4b15cdd3` (stock-ok images must not get fsqlite "never used"),
  bit-flip safety net `71c08c545` (one-bit structural flips **must** still be
  caught, not certified ok). Closed by `b1b5d01a4`.

### F. First-open repair for upgraders — `bd-zywqc.5` (closed)

Databases written by a pre-fix binary may carry latent corruption the old code
kept writing over. On the **first full open** of an unmarked DB, a bounded,
idempotent, interrupt-safe repair pass runs and records a marker so it never runs
again for that (database, version). Databases created by current code are stamped
at birth, so reopens short-circuit and the pass only ever touches genuinely
pre-fix files.

- Code: `crates/fsqlite-core/src/migration.rs`
  (`f32ce2aa6 feat(core): one-time idempotent first-open repair pass for upgraders`).
- Marker `<db>.fsqlite-migration-state` (`MIGRATION_MARKER_SUFFIX`, JSON:
  `last_upgrade_version` / `last_run_at` / `repairs_applied`); pre-mutation backup
  `<db>.pre-migration-bak` (+ `-wal`/`-shm`) (`PRE_MIGRATION_BACKUP_SUFFIX`);
  opt-out `FRANKENSQLITE_SKIP_MIGRATION=1` (`SKIP_MIGRATION_ENV`);
  `CURRENT_MIGRATION_VERSION = 1` (bump when a new repairable class is added).
- Interrupt-safe: repairs are atomic commits; marker + backup are temp-file +
  atomic rename; the marker is written **last**, so a kill leaves either the pre-
  or post-migration state, never a partial one.
- Tests: `crates/fsqlite-core/tests/migration_first_open_repair.rs`. Closed `5c79a1588`.

> **Note (interaction with E):** the same `page_count` clamp applies to the repair
> pass, so the migration no longer silently rewrites healthy DBs that merely carry
> stock-legal trailing slack.

### G. Format-version rollback safety — `bd-yaomh.6`

If a release changes the on-disk format (e.g. an added `.fsqlite-history` sidecar),
an older binary must **refuse** the newer file rather than corrupt it. A
format-version handshake is embedded so pre-change binaries detect-and-refuse with
a clear error.

- Code: `24e56a96b feat(format): refuse to open a newer .fsqlite than this build
  understands`. Error `SQLITE_OPEN_NEWER_FORMAT` in `crates/fsqlite-error/src/lib.rs`
  (`0x0E | (0x7F << 8)` → extended code `32526`; primary `SQLITE_CANTOPEN` = 14,
  private `ext_num` `0x7F`).
- Stock C SQLite still opens a format-stamped header (`b65a5f7b4`).

### H. Crash cleanup / process liveness — `bd-lyrja`, `bd-4dr7g`

A writer that dies mid-transaction leaves lock/slot state behind; the next opener
must reclaim it — but only if the owning process is genuinely dead, and without a
PID-reuse false-positive.

- **Linux (`bd-lyrja`):** `SharedPageLockTable` crash-cleanup with a process-liveness
  probe; matrix test `b9d7a7989 test(mvcc): bd-lyrja Linux crash-cleanup
  process-liveness matrix`.
- **macOS / Windows (`bd-4dr7g`):** `process_alive()` via `sysctl kinfo_proc`
  (Darwin) / `OpenProcess` + `GetProcessTimes` (Windows), with **reuse-safe birth
  tokens** so a recycled PID is not mistaken for the original owner
  (`f1a370455`, `402e09747 feat(vfs): macOS + Windows process-liveness probe for
  crash cleanup`).

---

## 4. Runbook — a DB went bad

1. **Detect.** Run `PRAGMA integrity_check`. Cross-check with stock:
   `sqlite3 <db> 'PRAGMA integrity_check'`. If fsqlite says corrupt but stock says
   `ok`, that is a **checker** bug (category E), not a data bug — capture the dump
   through `corruption_fingerprint::fingerprint()` and file it under `bd-zywqc`.
2. **Preserve.** Copy `<db>`, `<db>-wal`, `<db>-shm` aside before touching them.
   (The first-open repair also writes `<db>.pre-migration-bak*` automatically.)
3. **Recover.** For a pre-fix database, the fix path is a **clean full reopen**: the
   one-time repair pass (category F) runs, backs up, repairs, and stamps the marker.
   To force a re-run, delete `<db>.fsqlite-migration-state`. To skip it (and inspect
   raw), set `FRANKENSQLITE_SKIP_MIGRATION=1`.
4. **Verify.** Re-run `PRAGMA integrity_check` under **both** fsqlite and stock
   `sqlite3`; both must be `ok`. Confirm the marker records
   `last_upgrade_version == CURRENT_MIGRATION_VERSION`.
5. **Downgrade guard.** If an older binary refuses to open with
   `SQLITE_OPEN_NEWER_FORMAT`, that is category G working as intended — do **not**
   force it; use a build at or newer than the one that wrote the file.

---

## 5. Adversarial corpus — how each mode is exercised

Deterministic fault-injection lives under `bd-zywqc.14` and siblings:

- `crates/fsqlite-e2e/tests/adversarial_kill9_fsync.rs` — kill-9 during fsync.
- `crates/fsqlite-e2e/tests/adversarial_enospc.rs` — `SQLITE_FULL` + recovery
  (`19f890578`, `41ee2e879`).
- `crates/fsqlite-e2e/tests/adversarial_slow_disk.rs` — slow-disk timing.
- `crates/fsqlite-core/tests/adversarial_bit_flip.rs`,
  `adversarial_header_corruption.rs`, `adversarial_orphan_repair.rs` (`cafadba05`) —
  structural flips must be caught, not certified ok.
- `crates/fsqlite-harness/src/cross_process_crash_harness.rs`,
  `crash_recovery_parity.rs`, `adversarial_search.rs` — cross-process crash search
  and stock-parity replay.

Update this doc when a new adversarial signature is added or a new fix lands.

---

## 6. Open items (as of 2026-08-17)

- `bd-zywqc.20` (GH#329) — wire the public cross-process MVCC authority.
- `bd-zywqc.16` — concurrent `integrity_check` semantics (read-only check during
  live writes).
- `bd-zywqc.14` — adversarial corpus still filling out remaining exotic signatures.
- `bd-zywqc.11` / `.19` — production telemetry and the 48h swarm soak that
  certifies #70 closed end-to-end.

The authoritative supported-vs-unsupported statement remains
[`concurrency-contract.md`](./concurrency-contract.md) §"The concurrency contract".
