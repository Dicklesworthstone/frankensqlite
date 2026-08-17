//! Silo-style epoch group commit primitive (IMPL-16 / AG-5A).
//!
//! Implements the epoch-based group commit scheme from Silo (Tu et al.,
//! OSDI 2013). Instead of fsyncing the WAL per transaction, transactions
//! submit a *commit waiter* into a time-bucketed epoch; a background
//! advancer thread closes the current epoch every `epoch_duration_us`
//! microseconds, performs a single WAL flush for the entire bucket, and
//! then wakes every waiter that was submitted into the closed epoch.
//!
//! # Why
//!
//! MT-bench on current main shows fsqlite degrading 4->8 threads
//! (43k -> 30k writes/sec) because every transaction pays the per-txn WAL
//! flush cost. Group-committing within a ~40µs epoch window amortizes that
//! fsync across the bucket: throughput scales with batch size, and the
//! bounded epoch window caps tail latency.
//!
//! # Status: ABANDONED SCAFFOLD — do not wire (bd-rh3sr)
//!
//! This primitive was never wired into a real commit path and, per the
//! bd-rh3sr findings, it will not be. The live commit path
//! (`SimpleTransaction::commit`) already amortizes fsyncs across concurrent
//! committers through `fsqlite_pager::GroupCommitQueue` /
//! `fsqlite_wal::group_commit::GroupCommitConsolidator`, which additionally
//! carries the fairness budget and starvation guards this scaffold lacks.
//! Wiring Silo would either stack a second group-commit controller on top of
//! that (forbidden) or replace a richer controller with a poorer one.
//!
//! Crucially, the problem Silo was meant to solve is misattributed: the
//! 2026-04-23 8-thread profile shows the 1→2-thread throughput cliff is
//! dominated by *off-CPU lock wait* (the rightmost-leaf page-lock, or a
//! mis-tuned commit-path `inner.lock()`), not by per-transaction WAL fsync —
//! the only thing Silo amortizes. Silo therefore addresses neither plausible
//! cause. The real work is tracked as **bd-5fnex** (off-CPU cliff attribution
//! → GroupCommitQueue tuning / steal-on-same-page); the page-lock fast path is
//! **bd-77l3t**.
//!
//! The module is retained as a reference implementation of the Silo epoch
//! scheme and is marked `#[deprecated]` so any accidental wiring attempt fails
//! the build; its tests still exercise the primitive in isolation.
//!
//! # Shape
//!
//! - [`EpochGroupCommit::new`] spawns a background advancer task.
//! - [`EpochGroupCommit::submit`] returns a [`CommitWaiter`] that becomes
//!   ready exactly when the epoch it was submitted into is closed and
//!   flushed.
//! - [`EpochGroupCommit::advance_epoch`] is exposed for tests and for
//!   forcing an early flush from a synchronous context (e.g. an explicit
//!   COMMIT that does not want to pay the epoch latency).
//! - [`EpochGroupCommit::wait_for_commit`] blocks on a `Condvar` until the
//!   waiter's `ready` flag is set by the advancer.
//!
//! # Guarantees
//!
//! - Every submitted waiter is eventually resolved (advancer task runs
//!   forever until [`EpochGroupCommit`] is dropped).
//! - A waiter submitted at epoch `E` is resolved only after the flush
//!   callback for epoch `E` returns, so on wakeup the caller can assume
//!   its WAL frames are durable.
//! - Dropping an `EpochGroupCommit` signals the advancer to exit, drains
//!   any outstanding waiters (marking them ready so no one blocks
//!   forever), and joins the task.

// This module IS the deprecated scaffold; its own constructor, impls, and tests
// intentionally use the deprecated `EpochGroupCommit`. Silence the self-use here
// while callers in other modules still hit the `#[deprecated]` wiring warning.
#![allow(deprecated)]

use asupersync::runtime::{BlockingTaskHandle, Runtime, RuntimeBuilder};
use fsqlite_types::glossary::TxnId;
use parking_lot::{Condvar, Mutex};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

/// Handle returned from [`EpochGroupCommit::submit`]; becomes ready once
/// the epoch it was submitted into has been closed and flushed.
#[derive(Debug)]
pub struct CommitWaiter {
    /// Transaction id this waiter represents (informational; not used
    /// for correlation — the epoch stamp does that).
    pub txn_id: TxnId,
    /// Set to `true` by the advancer once the epoch containing this
    /// waiter has been flushed.
    pub ready: Arc<AtomicBool>,
    /// Epoch number at the moment of submission. The waiter is resolved
    /// when `current_epoch` advances strictly past this value.
    pub epoch_at_submit: u64,
    /// Shared condvar / mutex pair the advancer uses to wake us.
    notifier: Arc<WaiterNotifier>,
}

#[derive(Debug, Default)]
struct WaiterNotifier {
    /// Guards nothing payload-bearing — `ready` is atomic — but required
    /// by `Condvar` semantics.
    lock: Mutex<()>,
    cv: Condvar,
}

/// Internal bookkeeping for a single queued waiter.
#[derive(Debug)]
struct PendingEntry {
    ready: Arc<AtomicBool>,
    notifier: Arc<WaiterNotifier>,
    epoch_at_submit: u64,
}

/// Type of the flush callback invoked at each epoch boundary. In the
/// wired-in version this will be `|| wal.flush_and_fsync()`; for tests
/// and the MVP it is a no-op.
type FlushFn = Box<dyn Fn() + Send + Sync + 'static>;

/// Silo-style epoch group commit controller.
///
/// Abandoned scaffold — see the module docs. Superseded by
/// `fsqlite_pager::GroupCommitQueue`.
#[deprecated(
    note = "abandoned scaffold (bd-rh3sr): never wired; the live commit path \
            batches fsyncs via GroupCommitQueue, and the 1->2-thread cliff is \
            off-CPU lock wait, not per-txn fsync (bd-5fnex). Do not wire."
)]
pub struct EpochGroupCommit {
    /// Monotonically increasing epoch counter. Writers observe this at
    /// submit time; the advancer bumps it at each boundary.
    current_epoch: AtomicU64,
    /// Nominal epoch window. The advancer sleeps this long between
    /// boundaries.
    epoch_duration_us: u64,
    /// Waiters queued against the *current* epoch. Swapped out on each
    /// boundary.
    pending: Mutex<Vec<PendingEntry>>,
    /// Set to `true` on drop; the advancer observes this and exits.
    shutdown: Arc<AtomicBool>,
    /// Wakes the advancer task promptly during shutdown instead of waiting
    /// for the full epoch window to elapse.
    advancer_wake: Arc<WaiterNotifier>,
    /// Flush callback invoked at each boundary after the pending list is
    /// stolen. Arc-wrapped so `advance_epoch` (called synchronously from
    /// the caller) can share it with the background thread.
    flush: Arc<FlushFn>,
    /// Runtime that owns the advancer blocking task.
    advancer_runtime: Runtime,
    /// Handle for the advancer task. `None` after `Drop` joins it.
    ///
    /// Wrapped in `Mutex<Option<_>>` so we can hand out an `Arc<Self>`
    /// and then *stash* the handle after spawning the task, without
    /// needing `Arc::get_mut` (which races with the advancer's
    /// `Weak::upgrade`).
    advancer: Mutex<Option<BlockingTaskHandle>>,
}

impl std::fmt::Debug for EpochGroupCommit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EpochGroupCommit")
            .field("current_epoch", &self.current_epoch.load(Ordering::Relaxed))
            .field("epoch_duration_us", &self.epoch_duration_us)
            .field(
                "pending_count",
                &self.pending.try_lock().map_or(usize::MAX, |p| p.len()),
            )
            .finish_non_exhaustive()
    }
}

impl EpochGroupCommit {
    /// Create an `EpochGroupCommit` with a no-op flush callback.
    ///
    /// Spawns a background advancer task that closes the current epoch
    /// every `epoch_duration_us` microseconds. Use [`Self::new_with_flush`]
    /// to install a real WAL flush callback.
    #[must_use]
    pub fn new(epoch_duration_us: u64) -> Arc<Self> {
        Self::new_with_flush(epoch_duration_us, Box::new(|| {}))
    }

    /// Like [`Self::new`] but with a caller-supplied flush callback. The
    /// callback is invoked at each epoch boundary *before* waiters are
    /// marked ready, so when a waiter wakes up it can assume its WAL
    /// frames are durable.
    #[must_use]
    pub fn new_with_flush(epoch_duration_us: u64, flush: FlushFn) -> Arc<Self> {
        let advancer_runtime = RuntimeBuilder::new()
            .worker_threads(0)
            .blocking_threads(1, 1)
            .thread_name_prefix("silo-epoch")
            .build()
            .expect("silo epoch advancer runtime");

        // Build state first, then spawn the advancer with a Weak handle
        // so the background task does not keep the controller alive.
        let this = Arc::new(Self {
            current_epoch: AtomicU64::new(1),
            epoch_duration_us,
            pending: Mutex::new(Vec::new()),
            shutdown: Arc::new(AtomicBool::new(false)),
            advancer_wake: Arc::new(WaiterNotifier::default()),
            flush: Arc::new(flush),
            advancer_runtime,
            advancer: Mutex::new(None),
        });

        let weak = Arc::downgrade(&this);
        let wake = Arc::clone(&this.advancer_wake);
        let shutdown = Arc::clone(&this.shutdown);
        let advancer = this
            .advancer_runtime
            .spawn_blocking(move || {
                let epoch_window = Duration::from_micros(epoch_duration_us);
                loop {
                    if shutdown.load(Ordering::Acquire) {
                        return;
                    }
                    {
                        let mut guard = wake.lock.lock();
                        let _ = wake.cv.wait_for(&mut guard, epoch_window);
                    }
                    if shutdown.load(Ordering::Acquire) {
                        return;
                    }
                    let Some(state) = weak.upgrade() else {
                        return;
                    };
                    if state.shutdown.load(Ordering::Acquire) {
                        return;
                    }
                    state.advance_epoch();
                }
            })
            .expect("silo epoch advancer runtime must configure a blocking pool");

        // Stash the handle without requiring unique Arc ownership.
        *this.advancer.lock() = Some(advancer);
        this
    }

    /// Submit a transaction into the current epoch. Returns a
    /// [`CommitWaiter`] that will become ready once the epoch is flushed.
    ///
    /// # Ordering
    ///
    /// We load `current_epoch` *after* taking the pending lock. That way
    /// the advancer — which acquires the same lock before bumping the
    /// epoch — can never observe a waiter stamped with an already-closed
    /// epoch number.
    pub fn submit(&self, txn_id: TxnId) -> CommitWaiter {
        let ready = Arc::new(AtomicBool::new(false));
        let notifier = Arc::new(WaiterNotifier::default());
        let mut pending = self.pending.lock();
        let epoch_at_submit = self.current_epoch.load(Ordering::Acquire);
        pending.push(PendingEntry {
            ready: Arc::clone(&ready),
            notifier: Arc::clone(&notifier),
            epoch_at_submit,
        });
        drop(pending);
        CommitWaiter {
            txn_id,
            ready,
            epoch_at_submit,
            notifier,
        }
    }

    /// Close the current epoch: steal the pending list, bump the epoch
    /// counter, invoke the flush callback, then mark every stolen waiter
    /// ready.
    pub fn advance_epoch(&self) {
        let drained: Vec<PendingEntry> = {
            let mut pending = self.pending.lock();
            let drained = std::mem::take(&mut *pending);
            // Bump epoch *while holding the lock* so any submitter
            // blocked on `self.pending.lock()` sees the new epoch on
            // its subsequent `load`.
            self.current_epoch.fetch_add(1, Ordering::AcqRel);
            drained
        };

        // Flush *before* marking waiters ready so durability is
        // guaranteed on wakeup.
        (self.flush)();

        for entry in drained {
            entry.ready.store(true, Ordering::Release);
            // Acquire the waiter's lock before notify to avoid the
            // missed-wakeup race with a concurrent `wait_for_commit`
            // that has checked `ready` but not yet parked.
            let guard = entry.notifier.lock.lock();
            entry.notifier.cv.notify_all();
            drop(guard);
            let _ = entry.epoch_at_submit;
        }
    }

    /// Block until `waiter.ready` is observed `true`.
    pub fn wait_for_commit(&self, waiter: &CommitWaiter) {
        if waiter.ready.load(Ordering::Acquire) {
            return;
        }
        let mut guard = waiter.notifier.lock.lock();
        while !waiter.ready.load(Ordering::Acquire) {
            waiter.notifier.cv.wait(&mut guard);
        }
    }

    /// Like [`Self::wait_for_commit`] but returns `false` if `timeout`
    /// elapses before the waiter is resolved. Primarily useful for tests
    /// that need to assert "waiter is NOT yet ready".
    pub fn wait_for_commit_timeout(&self, waiter: &CommitWaiter, timeout: Duration) -> bool {
        if waiter.ready.load(Ordering::Acquire) {
            return true;
        }
        let mut guard = waiter.notifier.lock.lock();
        if waiter.ready.load(Ordering::Acquire) {
            return true;
        }
        let result = waiter.notifier.cv.wait_for(&mut guard, timeout);
        if result.timed_out() {
            waiter.ready.load(Ordering::Acquire)
        } else {
            true
        }
    }

    /// Current epoch counter (for tests / observability).
    #[must_use]
    pub fn current_epoch(&self) -> u64 {
        self.current_epoch.load(Ordering::Acquire)
    }
}

impl Drop for EpochGroupCommit {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        {
            let guard = self.advancer_wake.lock.lock();
            self.advancer_wake.cv.notify_all();
            drop(guard);
        }
        // Drain any stragglers so no one blocks forever.
        let drained: Vec<PendingEntry> = std::mem::take(&mut *self.pending.lock());
        for entry in drained {
            entry.ready.store(true, Ordering::Release);
            let guard = entry.notifier.lock.lock();
            entry.notifier.cv.notify_all();
            drop(guard);
        }
        let handle = self.advancer.lock().take();
        if let Some(h) = handle {
            h.cancel();
            h.wait();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64 as StdAtomicU64;
    use std::thread;
    use std::time::Instant;

    fn txn(id: u64) -> TxnId {
        TxnId::new(id).expect("test txn id")
    }

    /// Test 1: submit 100 txns, advance_epoch once => all 100 ready.
    #[test]
    fn advance_epoch_resolves_all_pending() {
        // Huge epoch window so the background advancer doesn't fire
        // during the test.
        let gc = EpochGroupCommit::new(10_000_000);
        let waiters: Vec<CommitWaiter> = (1..=100).map(|i| gc.submit(txn(i))).collect();

        // Nobody should be ready yet.
        for w in &waiters {
            assert!(!w.ready.load(Ordering::Acquire));
        }

        gc.advance_epoch();

        for w in &waiters {
            gc.wait_for_commit(w);
            assert!(w.ready.load(Ordering::Acquire));
        }
    }

    /// Test 2: submit a txn, do NOT advance => waiter blocks. Assert
    /// the waiter is not ready within 100µs.
    #[test]
    fn waiter_blocks_without_advance() {
        // Again a huge epoch window so the background thread can't
        // accidentally advance during the tight 100µs check.
        let gc = EpochGroupCommit::new(10_000_000);
        let w = gc.submit(txn(1));

        let start = Instant::now();
        let resolved = gc.wait_for_commit_timeout(&w, Duration::from_micros(100));
        let elapsed = start.elapsed();

        assert!(
            !resolved,
            "waiter should not have been resolved without advance_epoch; elapsed={elapsed:?}"
        );
        assert!(!w.ready.load(Ordering::Acquire));
    }

    /// Test 3: 4 submitter threads, 1 advancer thread; every submitted
    /// waiter is eventually resolved.
    #[test]
    fn multi_threaded_submit_and_advance() {
        let gc = EpochGroupCommit::new(10_000_000); // bg thread idle
        let per_thread = 200_u64;

        let handles: Vec<_> = (0..4_u64)
            .map(|tid| {
                let gc = Arc::clone(&gc);
                thread::spawn(move || {
                    let mut waiters = Vec::with_capacity(per_thread as usize);
                    for i in 0..per_thread {
                        let id = tid * 10_000 + i + 1;
                        waiters.push(gc.submit(txn(id)));
                    }
                    waiters
                })
            })
            .collect();

        // Dedicated advancer thread: keeps bumping the epoch until the
        // total resolved count reaches the expected value.
        let resolved_total = Arc::new(StdAtomicU64::new(0));
        let expected = 4 * per_thread;
        let advancer = {
            let gc = Arc::clone(&gc);
            let resolved_total = Arc::clone(&resolved_total);
            thread::spawn(move || {
                while resolved_total.load(Ordering::Acquire) < expected {
                    gc.advance_epoch();
                    thread::sleep(Duration::from_micros(50));
                }
            })
        };

        // Collect waiters back from the submitters and wait on each.
        for h in handles {
            let waiters = h.join().expect("submitter thread");
            for w in &waiters {
                gc.wait_for_commit(w);
                assert!(w.ready.load(Ordering::Acquire));
                resolved_total.fetch_add(1, Ordering::AcqRel);
            }
        }

        advancer.join().expect("advancer thread");
        assert_eq!(resolved_total.load(Ordering::Acquire), expected);
    }
}
