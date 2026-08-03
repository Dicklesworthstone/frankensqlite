# Injection Hook Plan: Batched Append & Publish Paths

> **Bead:** bd-db300.7.2.2
> **Date:** 2026-03-22
> **Depends on:** bd-db300.7.2.1 (fault matrix)
> **Author:** Claude Opus 4.6 performance-correctness agent

## Design Principle

Each hook is a `cfg(test)` static `AtomicBool` + optional `AtomicU32` counter. Production
code pays zero cost. Test code sets the flag, runs the scenario, and checks the proof
obligation. No dynamic dispatch, no trait objects, no runtime overhead.

```rust
#[cfg(test)]
pub(crate) static FAULT_INJECT_AFTER_WAL_APPEND: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
```

Hooks that need to fire on the Nth invocation use an `AtomicU32` countdown:

```rust
#[cfg(test)]
pub(crate) static FAULT_INJECT_WAL_APPEND_ERROR_COUNTDOWN: std::sync::atomic::AtomicU32 =
    std::sync::atomic::AtomicU32::new(0);
```

---

## Hook Definitions

### H1. `FAULT_INJECT_AFTER_WAL_APPEND`

| Field | Value |
|-------|-------|
| **Fault case** | F1 — crash after frame write, before sync |
| **File** | `crates/fsqlite-wal/src/wal.rs` |
| **Insertion point** | Line 1045, immediately after `self.append_prepared_frame_bytes(cx, ...)` returns `Ok(())` |
| **Guard code** | `#[cfg(test)] if FAULT_INJECT_AFTER_WAL_APPEND.load(Ordering::Acquire) { return Err(FrankenError::Io(std::io::Error::new(std::io::ErrorKind::Other, "fault_inject: crash after append"))); }` |
| **Effect** | Frames written to WAL file but sync never called. Simulates power loss between write and fsync. |
| **Proof obligation** | After re-opening the WAL (`WalFile::open()`), `frame_count` must equal the count BEFORE the injected append (torn frames discarded). Checksum chain must be valid from frame 0 to the surviving tail. |
| **Test name** | `test_fault_crash_after_wal_append_recovers_cleanly` |

---

### H2. `FAULT_INJECT_DURING_WAL_SYNC`

| Field | Value |
|-------|-------|
| **Fault case** | F2 — sync call interrupted |
| **File** | `crates/fsqlite-wal/src/wal.rs` |
| **Insertion point** | Line 1133, inside `sync()`, before `self.file.sync(cx, flags)` |
| **Guard code** | `#[cfg(test)] if FAULT_INJECT_DURING_WAL_SYNC.load(Ordering::Acquire) { return Err(FrankenError::Io(std::io::Error::new(std::io::ErrorKind::Other, "fault_inject: sync interrupted"))); }` |
| **Effect** | Sync call fails. Caller must handle the error. Frames may or may not be durable depending on OS write-back state. |
| **Proof obligation** | Caller propagates the error to the transaction. Transaction is rolled back. On restart, WAL recovery produces a valid chain (possibly missing the un-synced frames). |
| **Test name** | `test_fault_sync_interrupted_transaction_rolls_back` |

---

### H3. `FAULT_INJECT_AFTER_FLUSH_BEFORE_PUBLISH`

| Field | Value |
|-------|-------|
| **Fault case** | F3 — crash between group-commit flush and epoch notification |
| **File** | `crates/fsqlite-pager/src/pager.rs` |
| **Insertion point** | Line 3685, after `with_wal_backend(wal_backend, \|wal\| wal.sync(cx))` succeeds, before line 3761 `consolidator.complete_flush()` |
| **Guard code** | `#[cfg(test)] if crate::fault_hooks::FAULT_INJECT_AFTER_FLUSH_BEFORE_PUBLISH.load(Ordering::Acquire) { return Err(FrankenError::Io(std::io::Error::new(std::io::ErrorKind::Other, "fault_inject: crash after flush"))); }` |
| **Effect** | WAL frames durable on disk. In-memory state (commit_seq, snapshot plane) never updated. Waiters receive error. Pipelined next_epoch_batches are lost. |
| **Proof obligation** | After restart, `WalFile::open()` recovers all frames from the completed flush. `reload_memdb_from_pager()` rebuilds correct state. No waiter thread hangs (Condvar error path wakes them). |
| **Test name** | `test_fault_crash_after_flush_recovers_durable_frames` |

---

### H4. `FAULT_INJECT_DURING_PHASE_C`

| Field | Value |
|-------|-------|
| **Fault case** | F4 — crash during commit Phase C publication |
| **File** | `crates/fsqlite-pager/src/pager.rs` |
| **Insertion point** | Line ~4460, inside `SimpleTransaction::commit()`, after `inner.lock()` re-acquired in Phase C but before snapshot publish completes |
| **Guard code** | `#[cfg(test)] if crate::fault_hooks::FAULT_INJECT_DURING_PHASE_C.load(Ordering::Acquire) { return Err(FrankenError::Io(std::io::Error::new(std::io::ErrorKind::Other, "fault_inject: crash during phase C"))); }` |
| **Effect** | WAL frames durable, but commit_seq not published. On restart, the durable frames still exist in the WAL. |
| **Proof obligation** | After restart, commit_seq reflects all durable WAL transactions. No duplicate or missing commits. |
| **Test name** | `test_fault_crash_during_phase_c_publishes_on_recovery` |

---

### H5. `FAULT_INJECT_WAL_APPEND_ERROR` (countdown variant)

| Field | Value |
|-------|-------|
| **Fault case** | F5 — flusher I/O error during append_frames |
| **File** | `crates/fsqlite-wal/src/wal.rs` |
| **Insertion point** | Line 1001, at entry of `append_frames()` |
| **Guard code** | `#[cfg(test)] { let c = FAULT_INJECT_WAL_APPEND_ERROR_COUNTDOWN.load(Ordering::Acquire); if c > 0 && FAULT_INJECT_WAL_APPEND_ERROR_COUNTDOWN.compare_exchange(c, c-1, Ordering::AcqRel, Ordering::Relaxed).is_ok() && c == 1 { return Err(FrankenError::Busy); } }` |
| **Effect** | The Nth call to `append_frames()` fails with Busy. Flusher retries (pager.rs:3658-3724, `MAX_FLUSH_RETRIES=10`). |
| **Proof obligations** | (1) Waiters receive the same error if retries exhausted. (2) No partial frames in WAL. (3) Condvar wakes all waiters. (4) Subsequent flush succeeds (queue not poisoned). |
| **Test name** | `test_fault_wal_append_busy_retries_and_recovers` |

---

### H6. `FAULT_INJECT_EPOCH_SKIP`

| Field | Value |
|-------|-------|
| **Fault case** | F6 — epoch counter anomaly |
| **File** | `crates/fsqlite-wal/src/group_commit.rs` |
| **Insertion point** | Line 815, inside `begin_flush()`, after `self.epoch += 1` |
| **Guard code** | `#[cfg(test)] if FAULT_INJECT_EPOCH_SKIP.load(Ordering::Acquire) { self.epoch += 1; /* skip one epoch */ }` |
| **Effect** | Epoch jumps by 2. Waiters targeting epoch N+1 may see completed_epoch = N+2 before their target is reached. |
| **Proof obligation** | No waiter hangs. The `>=` guard in `wait_for_epoch_outcome()` (pager.rs:3819) handles the skip. All waiters unblock with success or timeout. |
| **Test name** | `test_fault_epoch_skip_no_waiter_hang` |

---

### H7. `FAULT_INJECT_CHECKPOINT_TRUNCATE_RACE`

| Field | Value |
|-------|-------|
| **Fault case** | F7 — checkpoint truncation racing with WAL append |
| **File** | `crates/fsqlite-wal/src/wal.rs` |
| **Insertion point** | Line 1141, at entry of `reset()` |
| **Guard code** | `#[cfg(test)] if FAULT_INJECT_CHECKPOINT_TRUNCATE_RACE.load(Ordering::Acquire) { /* signal companion thread to start append */ FAULT_INJECT_CHECKPOINT_TRUNCATE_RACE_BARRIER.wait(); }` |
| **Effect** | Forces checkpoint reset and WAL append to race. Requires a companion `Barrier` static to synchronize two threads. |
| **Proof obligations** | (1) File lock prevents actual overlap. (2) If lock is somehow bypassed (injected), readers see consistent data. (3) No phantom frames after truncation. |
| **Test name** | `test_fault_checkpoint_truncate_vs_append_serialized_by_lock` |

---

### H8. `FAULT_INJECT_CLOSE_WAL_BEFORE_COMMIT`

| Field | Value |
|-------|-------|
| **Fault case** | F8 / bd-zna34 — bad file descriptor during commit |
| **File** | `crates/fsqlite-vfs/src/unix.rs` |
| **Insertion point** | Line 1555, at entry of `UnixFile::close()` |
| **Guard code** | `#[cfg(test)] { if FAULT_INJECT_CLOSE_WAL_BEFORE_COMMIT.load(Ordering::Acquire) { tracing::error!(fd = ?self.file, path = %self.path.display(), n_ref = ?self.inode_info.lock().unwrap().n_ref, "FAULT_INJECT: premature WAL close"); } }` |
| **Effect** | Diagnostic logging only — does not inject failure, but captures the exact state when close() is called. The implementer uses this to trace which thread/connection is closing the WAL fd and whether any other thread's sync() is in-flight. |
| **Extended variant** | `FAULT_INJECT_CLOSE_WAL_FORCE_EBADF`: After `close()` completes, replace `self.file` with an `Arc::new(File::from(OwnedFd::from_raw_fd(-1)))` to force EBADF on subsequent operations from other Arc clones. (Requires `unsafe` — test-only.) |
| **Proof obligation** | The diagnostic hook reveals the sequence: (a) which thread closes the WAL file, (b) whether n_ref drops to 0, (c) whether the inode entry is removed, (d) whether another thread subsequently calls sync(). |
| **Test name** | `test_fault_diagnose_ebadf_during_concurrent_commit` |

---

### H9. `FAULT_INJECT_CRASH_HEADER_TRUNCATE`

| Field | Value |
|-------|-------|
| **Fault case** | F9 — crash between WAL header rewrite and frame truncation |
| **File** | `crates/fsqlite-wal/src/wal.rs` |
| **Insertion point** | Line 1157, after new header is written, before line 1159 truncation |
| **Guard code** | `#[cfg(test)] if FAULT_INJECT_CRASH_HEADER_TRUNCATE.load(Ordering::Acquire) { return Err(FrankenError::Io(std::io::Error::new(std::io::ErrorKind::Other, "fault_inject: crash between header and truncate"))); }` |
| **Effect** | WAL has new header (new salts) but old frames (old salts). |
| **Proof obligation** | Recovery produces zero frames (salt mismatch terminates scan). Database state equals last checkpoint. |
| **Test name** | `test_fault_crash_between_header_and_truncate_recovers_to_checkpoint` |

---

### H10. `FAULT_INJECT_FEC_HOOK_FAILURE`

| Field | Value |
|-------|-------|
| **Fault case** | F10 — FEC encoding fails after WAL append |
| **File** | `crates/fsqlite-core/src/wal_adapter.rs` |
| **Insertion point** | Line ~837, before FEC encoding block |
| **Guard code** | `#[cfg(test)] if FAULT_INJECT_FEC_HOOK_FAILURE.load(Ordering::Acquire) { tracing::warn!("fault_inject: FEC hook failure simulated"); /* skip FEC but don't fail append */ }` |
| **Effect** | WAL frames durable but no FEC repair symbols generated. |
| **Proof obligation** | Data integrity preserved. WAL recoverable via standard checksum chain. Only self-healing is degraded. |
| **Test name** | `test_fault_fec_failure_does_not_lose_data` |

---

### H11. `drop_waiter_notify`

| Field | Value |
|-------|-------|
| **Fault case** | F11 — suppressed delivery to an active group-commit waiter |
| **File** | `crates/fsqlite-pager/src/pager.rs` |
| **Insertion point** | `GroupCommitQueue::publish_completed_epoch()`, after the completed-epoch store and before targeted waiter delivery |
| **Guard code** | `maybe_inject_drop_waiter_notify(completed_epoch, wait_strategy, notification_surface)` selects delivery suppression; `signal_completed_epoch_waiters(..., false)` still advances every live targeted keyed slot's generation. |
| **Effect** | The active wait strategy's direct notification is suppressed. The keyed waiter must use its 200ms bounded timeout, observe the generation change, and recheck the completed epoch. |
| **Proof obligation** | Once the publisher reaches the completed-epoch seam, a waiter held after its fast generation check and before timeout construction recovers exactly once through its bounded timeout path. The publisher is joined before assertions so the test leaves no background worker. Arbitrary synchronous publisher deadlocks require separate process-isolated timeout coverage. |
| **Test name** | `test_fault_drop_waiter_notify_recovers_via_timeout` |

---

## Implementation File Layout

All statics live in dedicated modules to keep production code clean:

```
crates/fsqlite-wal/src/fault_hooks.rs          ← H1, H2, H5, H6, H9
crates/fsqlite-pager/src/fault_hooks.rs        ← H3, H4, H7, H11
crates/fsqlite-vfs/src/fault_hooks.rs          ← H8
crates/fsqlite-core/src/fault_hooks.rs         ← H10
```

Each module:
```rust
//! Fault injection hooks for testing crash/fault scenarios.
//! All hooks are `cfg(test)` — zero production cost.

#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

#[cfg(test)]
pub(crate) static FAULT_INJECT_AFTER_WAL_APPEND: AtomicBool = AtomicBool::new(false);
// ... etc
```

Guard at each insertion point:
```rust
#[cfg(test)]
{
    if crate::fault_hooks::FAULT_INJECT_AFTER_WAL_APPEND.load(
        std::sync::atomic::Ordering::Acquire,
    ) {
        return Err(FrankenError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "fault_inject: crash after append",
        )));
    }
}
```

## Test Harness Pattern

Each test follows:

```rust
#[test]
fn test_fault_crash_after_wal_append_recovers_cleanly() {
    // 1. Setup: create WAL with N known frames
    let (dir, wal_path) = setup_wal_with_frames(10);

    // 2. Arm hook
    FAULT_INJECT_AFTER_WAL_APPEND.store(true, Ordering::Release);

    // 3. Attempt append — should fail
    let result = wal.append_frames(&cx, &new_frames);
    assert!(result.is_err());

    // 4. Disarm hook
    FAULT_INJECT_AFTER_WAL_APPEND.store(false, Ordering::Release);

    // 5. Recovery: re-open WAL
    let recovered = WalFile::open(&cx, vfs.open(&wal_path)?);

    // 6. Proof: frame_count == 10 (pre-crash), checksum chain valid
    assert_eq!(recovered.frame_count(), 10);
    assert!(recovered.checksum_chain_valid());
}
```

## Priority Order for Implementation

1. **H1** + **H2** (WAL append/sync) — fundamental durability
2. **H8** (EBADF diagnostic) — unblocks bd-zna34 root cause
3. **H3** + **H4** (group-commit publish) — publish path integrity
4. **H5** (append error retry) — flusher resilience
5. **H9** (header/truncate race) — checkpoint safety
6. **H11** (waiter-delivery loss) — liveness under fault
7. **H6**, **H7**, **H10** — secondary / rare paths
