# Fault Matrix: Batched Append & Publish Paths

> **Bead:** bd-db300.7.2.1 / bd-966ja
> **Date:** 2026-03-22
> **Author:** Claude Opus 4.6 performance-correctness agent
> **Scope:** C1 (batched WAL append), C2 (checksum/serialization outside publish),
>   checkpoint, and the bd-zna34 EBADF failure path.

## Path Inventory

| ID   | Path Name                     | Entry Point (file:line)                                      | Hot? |
|------|-------------------------------|--------------------------------------------------------------|------|
| P1   | Group-commit flusher          | `pager.rs:3560` `SubmitOutcome::Flusher`                     | Yes  |
| P2   | Group-commit waiter           | `pager.rs:3861` `SubmitOutcome::Waiter` (shifted from 3803)  | Yes  |
| P3   | WAL append_frames             | `wal.rs:1001` `WalFile::append_frames`                       | Yes  |
| P4   | WAL sync                      | `wal.rs:1132` `WalFile::sync`                                | Yes  |
| P5   | Checkpoint backfill           | `checkpoint_executor.rs:104`                                 | No   |
| P6   | Checkpoint WAL reset/truncate | `wal.rs:1141` `WalFile::reset`                               | No   |
| P7   | Epoch pipelining              | `pager.rs:3547-3556` next_epoch_batches submit during FLUSHING | Yes  |
| P8   | WalBackendAdapter publish     | `wal_adapter.rs:830-868` `record_appended_frames` + FEC      | Yes  |
| P9   | Phase C publish (distributed) | `pager.rs:4470+` commit_seq in Phase A; snapshot via `publish_completed_epoch` at 3823 | Yes  |
| P10  | VFS file open (IoUring+Unix)  | `uring.rs:632-672` double-fd open                            | Startup |
| P11  | VFS inode table close         | `unix.rs:1555-1581` refcount decrement + maybe_remove        | Close |

---

## Fault Cases

### F1. Crash during WAL frame write (P3)

**Trigger:** Power loss / process kill between `append_prepared_frame_bytes()` (wal.rs:1045)
and `sync()` (wal.rs:1132).

**Symptom:** Partial frame(s) on disk. Running checksum chain is broken at the torn frame.

**Recovery contract:** `WalFile::open()` (wal.rs:498-600) re-scans frames, terminates the
valid chain at the last frame whose checksum matches the running chain. Partial/torn
frames are discarded. This is the standard SQLite WAL recovery model.

**Injection hook:** `fault_inject_after_wal_append` — insert between wal.rs:1045 and the
caller's sync(). Return `Err(FrankenError::Io(...))` to simulate crash before sync.

**Proof obligation:** After injected failure, re-opening the WAL must produce
`frame_count <= pre-crash frame_count` and the checksum chain must be valid.

---

### F2. Crash during WAL sync (P4)

**Trigger:** Process kill during `file.sync(cx, flags)` (wal.rs:1133).

**Symptom:** fsync returns but some sectors may not have reached stable storage (depends
on hardware). Frames are either fully durable or fully lost (sector-atomic assumption).

**Recovery contract:** Same as F1 — checksum chain scan discards any incomplete frames.

**Injection hook:** `fault_inject_during_wal_sync` — drop the sync call and simulate
process restart.

**Proof obligation:** Re-opened WAL has consistent checksum chain. No phantom frames
survive that weren't fully written.

---

### F3. Crash between group-commit flush completion and epoch notification (P1→P7)

**Trigger:** Process kill after WAL sync completes (pager.rs:3685) but before
`publish_completed_epoch()` (pager.rs:3771) notifies waiters.

**Symptom:** WAL frames are durable on disk. Waiters never received notification. On
restart, the frames are recoverable via WAL scan, but in-memory state (commit_seq,
snapshot plane) was never updated.

**Recovery contract:** On restart, `WalFile::open()` rediscovers all durable frames.
`reload_memdb_from_pager()` rebuilds state from the durable WAL + db file.

**Injection hook:** `fault_inject_after_flush_before_publish` — insert between
pager.rs:3685 (sync) and pager.rs:3761 (complete_flush). Kill process.

**Proof obligation:** After restart, all committed frames from the completed flush
are visible. Waiter transactions that were "in-flight" are rolled back (they never
committed — the group-commit's frames are durable but the waiters' phase-C
publication never happened).

**Subtlety:** With epoch pipelining (P7), `next_epoch_batches` may contain frames
submitted during the flush. These are NOT durable (they were never written to the WAL)
and must be silently dropped on restart.

---

### F4. Crash during Phase C publication (P9)

**Trigger:** Process kill between inner.lock() re-acquisition (pager.rs:4446) and
snapshot plane publication (pager.rs:4509).

**Symptom:** WAL frames are durable. `commit_seq` was incremented in-memory but the
published snapshot may not reflect it.

**Recovery contract:** On restart, WAL recovery re-establishes the correct state.
The `commit_seq` is derived from the durable frame chain, not from in-memory counters.

**Injection hook:** `fault_inject_during_phase_c` — insert between commit_seq
update and snapshot plane publication.

**Proof obligation:** After restart, commit_seq matches the number of committed
transactions recoverable from WAL. No "ghost" commits visible.

---

### F5. Flusher panics or returns error mid-batch (P1)

**Trigger:** I/O error during `wal.append_frames()` (pager.rs:3677) or
`inner.db_file.lock(cx, LockLevel::Exclusive)` (pager.rs:3670) fails.

**Symptom:** The flusher records the failure in `failed_epochs` (pager.rs,
`GroupCommitQueue::failed_epochs`) and notifies waiters. Waiter threads receive
the error and propagate it to their callers.

**Current code path:** pager.rs:3708-3724 retry loop with exponential backoff
for Busy errors. After `MAX_FLUSH_RETRIES` (10), the error propagates.

**Injection hook:** `fault_inject_wal_append_error` — make `wal.append_frames()`
return `Err(FrankenError::Busy)` N times, then succeed or fail permanently.

**Proof obligations:**
1. Waiter threads receive the same error (not a stale success).
2. No partial frames written to WAL (append_frames must be atomic-or-nothing).
3. The Condvar waiters are all woken (no stuck threads).
4. Subsequent flushes can succeed (queue is not poisoned).

---

### F6. Waiter epoch mismatch / stale epoch (P2)

**Trigger:** Race between epoch increment (consolidator.begin_flush → epoch++)
and waiter's epoch capture (pager.rs:3554 `consolidator.epoch()`).

**Symptom:** Waiter captures epoch N, but the flusher has already advanced to N+1.
Waiter then waits for epoch N+1 which may have already completed.

**Current guard:** `wait_for_epoch_outcome()` (pager.rs:3819) checks
`completed_epoch >= target_epoch` before blocking. If the epoch already
completed, it returns immediately.

**Injection hook:** `fault_inject_epoch_skip` — artificially increment epoch
by 2 instead of 1 to test the `>=` guard.

**Proof obligation:** No waiter thread hangs indefinitely. Every waiter
either gets success or a propagated error.

---

### F7. Checkpoint WAL truncation racing with concurrent append (P5+P6 vs P3)

**Trigger:** Checkpoint calls `wal.reset(truncate=true)` (wal.rs:1159-1164) while
a concurrent writer's group-commit is preparing to append frames.

**Current guard:** The flusher acquires `Exclusive` file lock (pager.rs:3670)
before WAL I/O, and checkpoint also acquires an exclusive lock. Only one can
hold it at a time.

**Race window:** Between the flusher releasing Exclusive→Reserved (pager.rs:3698)
and the checkpoint acquiring Exclusive. If the checkpoint truncates the WAL, any
subsequent reader that tries to read "committed" frames from the truncated region
gets garbage or EOF.

**Injection hook:** `fault_inject_checkpoint_between_unlock_and_next_append` —
schedule checkpoint truncation in the window between flusher's unlock and the
next commit's lock acquisition.

**Proof obligations:**
1. Readers that opened before truncation see a consistent snapshot.
2. Writers that begin after truncation get a fresh WAL with valid header.
3. No phantom reads of post-truncation garbage.

---

### F8. **bd-zna34: Bad file descriptor (EBADF) during persistent benchmark commit** (P10+P11+P4)

**Error signature:** `COMMIT failed: Io(Os { code: 9, kind: Uncategorized, message: "Bad file descriptor" })`

**Context:** Multi-thread persistent benchmark. Each thread opens its own
`Connection::open(&path)` which creates:
- `IoUringVfs::open()` (uring.rs:632-672): opens TWO fd's per file
  - `UnixFile` via `self.unix.open()` → shared through global inode table `Arc<File>`
  - `AsupersyncIoUringFile` via `open_asupersync_backend()` → independent fd

**Root cause: CONFIRMED and FIXED (2026-03-22).**

The WAL file was opened `READONLY` in two connection.rs functions:
- `install_existing_wal_backend_with_vfs()` (line 1174): tried READONLY before READWRITE
- `wal_file_present_with_vfs()` (line 1205): probe opened READONLY

On the first open for an inode, the global `InodeInfo` table installs this
READONLY fd as the canonical fd shared across all connections. When subsequent
connections clone this fd via the fast path, they inherit the read-only handle.
The WAL group commit flusher calls `pwrite()` on this read-only fd, which
returns EBADF on Linux (pwrite on O_RDONLY fd = EBADF).

**Diagnostic evidence:**
```
fd=6 closed=false path=/tmp/.tmpSBeP7K-wal
fcntl(F_GETFD)=1 /proc/self/fd exists=true
Arc<File> strong=3 Arc<InodeInfo> strong=3
```
The fd was VALID but READONLY → pwrite returned EBADF.

**Fix:** Changed both functions to open READWRITE first, with READONLY as
fallback only for read-only filesystems. Verified with full Criterion
benchmark (14+ iterations, 0 errors) and with `--no-default-features`
(no io_uring) confirming H3 was NOT the cause.

**Hypothesis disposition:**
- H1 (inode table removal): NOT the primary cause (maybe_remove_exact_when_idle guard was correct)
- H2 (NamedTempFile race): NOT the primary cause (bug occurs within single iteration)
- H3 (asupersync fd): RULED OUT (reproduces without io_uring)
- H4 (stale queue): NOT the primary cause (different temp paths per iteration)

**Original hypotheses preserved below for historical reference:**

H1 (DEBUNKED): Inode table premature removal under concurrent close/re-open.

H2 (DEBUNKED): NamedTempFile deletion race.

H3 (RULED OUT): AsupersyncIoUringFile fd lifetime vs IoUringFile.inner close.

H4 (NOT PRIMARY): Global GroupCommitQueue outlives file handles.

**H5 (LOW-MEDIUM): Auto-checkpoint WAL reset invalidating concurrent WAL state.**
`maybe_run_adaptive_autocheckpoint()` (connection.rs:11080) runs after each commit.
It calls `pager.checkpoint(&cx, mode)` which can reset or truncate the WAL via
`WalFile::reset()` (wal.rs:1141). If Thread A's auto-checkpoint resets/truncates
the shared WAL file while Thread B's WAL backend is still referencing the old
state, Thread B's next WAL sync could fail. However, the checkpoint acquires
an Exclusive file lock (checkpoint_executor.rs:104-158), and WAL append also
acquires Exclusive (pager.rs:3670), so they shouldn't overlap. The race window
would have to be between lock release and re-acquisition across threads.

**Recommended investigation steps:**
1. Add `tracing::warn!` in `UnixFile::close()` logging the fd number, path, and n_ref.
2. Add `tracing::warn!` in `maybe_remove()` when n_ref reaches 0, logging the inode key.
3. Add guard in `GroupCommitQueue` to detect stale entries (e.g., check if the
   underlying file is still valid before flushing).
4. Build a focused repro: 2 threads, single iteration, with RUST_LOG=debug tracing.
5. Check whether auto-checkpoint runs during the benchmark by adding tracing to
   `maybe_run_adaptive_autocheckpoint()` and watching for interleaving with commits.

**Injection hook:** `fault_inject_close_wal_before_commit` — close one thread's WAL
file handle immediately before another thread's commit attempts sync.

---

### F9. Checksum chain corruption from concurrent WAL header rewrite (P6 vs P3)

**Trigger:** `WalFile::reset()` (wal.rs:1157) writes a new WAL header with new
salts. A concurrent reader scanning the WAL sees the new salts for the header
but old salts in pre-existing frames.

**Current guard:** The checksum scan in `WalFile::open()` uses the salts from
the header. If frames have different salts, they're treated as the end of the
valid chain (wal.rs:527-597, salt mismatch terminates scan).

**Race window:** Between writing the new header and truncating old frames.
If the process crashes here, the old frames have old salts, the new header
has new salts. Recovery correctly ignores the old frames.

**Injection hook:** `fault_inject_crash_between_header_write_and_truncate` —
inject between wal.rs:1157 (header write) and wal.rs:1159 (truncate).

**Proof obligation:** Recovery produces zero frames (all old frames have
mismatched salts vs new header). Database is in the state of the last
checkpoint.

---

### F10. FEC encoding failure during publish (P8)

**Trigger:** FEC commit hook in `WalBackendAdapter::append_frames()`
(wal_adapter.rs:837-864) fails after frames are already appended to WAL.

**Symptom:** WAL frames are durable, but FEC repair symbols are not
generated. Self-healing capability is degraded but data is not lost.

**Current code path:** FEC hook failure is logged but does not roll back
the WAL append (the frames are already durable).

**Injection hook:** `fault_inject_fec_hook_failure` — make the FEC hook
return an error after WAL append succeeds.

**Proof obligation:** Data integrity is preserved even without FEC symbols.
The WAL can be recovered using standard checksum-chain scanning.

---

### F11. Lost group-commit waiter delivery (P1+P2)

**Trigger:** A completion is published after a waiter has checked its keyed
generation, but the corresponding `Notify` delivery is lost. Without a bounded
eventcount recheck, the waiter could remain parked indefinitely.

**Current guard:** `publish_completed_epoch()` stores the completed epoch before
signaling its targeted keyed slot. `wait_for_epoch_outcome_async()` samples the
slot generation before checking the epoch, then uses a 200ms bounded wait. A
timeout rechecks the generation and completed epoch, so both a missed delivery
and a delivery that races registration remain observable.

**Injection hook:** `drop_waiter_notify` — preserve the completed-epoch store and
targeted keyed-generation advance while suppressing the active wait strategy's
direct delivery.

**Proof obligation:** A waiter already inside the exact keyed wait boundary
recovers through the timeout-generation path and observes the completed epoch.
The publisher is joined before assertions, leaving no background worker behind.
An arbitrary synchronous publisher deadlock is outside this injected-delivery
keeper and requires process-isolated timeout coverage.

---

## Injection Hook Summary

| Hook ID | Fault Case | Code Location | Technique |
|---------|-----------|---------------|-----------|
| `fault_inject_after_wal_append` | F1 | wal.rs:1045→caller | Return Err before sync |
| `fault_inject_during_wal_sync` | F2 | wal.rs:1133 | Drop sync, simulate restart |
| `fault_inject_after_flush_before_publish` | F3 | pager.rs:3685→3761 | Kill process |
| `fault_inject_during_phase_c` | F4 | pager.rs:4446→4509 | Kill between update+publish |
| `fault_inject_wal_append_error` | F5 | pager.rs:3677 | Inject Busy/Io error |
| `fault_inject_epoch_skip` | F6 | consolidator epoch | Increment by 2 |
| `fault_inject_checkpoint_truncate_race` | F7 | between unlock→lock | Schedule checkpoint |
| `fault_inject_close_wal_before_commit` | F8/zna34 | unix.rs:1555 | Close WAL fd pre-commit |
| `fault_inject_crash_header_truncate` | F9 | wal.rs:1157→1159 | Crash between ops |
| `fault_inject_fec_hook_failure` | F10 | wal_adapter.rs:837 | Error after append |
| `drop_waiter_notify` | F11 | `publish_completed_epoch` waiter delivery | Advance targeted eventcount; suppress direct delivery |

---

## Cross-References

- **bd-zna34** (P0 bug): Covered by F8. **FIXED** (READONLY canonical fd poisoning).
- **bd-db300.3.1** (C1: batched WAL append): Covered by F1, F2, F3, F5.
- **bd-db300.3.2** (C2: checksum outside publish): Covered by F3, F4, F8.
- **bd-db300.5.3.1** (metadata publication): F3, F4 are the publish-path faults.
- **Future C5 work** (advanced group commit): F6, F7, F11 are scaling faults.

## Assumptions Ledger

1. **Sector-atomic writes:** WAL frames are assumed to be sector-aligned and
   each sector write is atomic. If torn at sub-sector granularity, the checksum
   chain catches it on recovery.

2. **fsync durability:** After `file.sync()` returns `Ok(())`, frames are on
   stable storage. If the hardware lies (write cache without battery backup),
   all bets are off — this is outside our fault model.

3. **POSIX fd semantics (hardened by bd-zna34 fix):** Closing an fd does not affect other fds for the same
   file opened via `open()`. The `Arc<File>` model in unix.rs depends on this.

4. **Single-process model:** The current fault matrix assumes all writers are
   in the same process (sharing the global inode table and group commit queues).
   Multi-process MVCC (bd-2l5jk) will require a separate fault matrix.

5. **No filesystem-level corruption:** We assume the filesystem does not silently
   corrupt data that was successfully fsync'd. Bit-rot detection is handled by
   the RaptorQ/FEC layer, not the fault injection layer.
