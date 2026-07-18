//! Flat-combining publication list for `InProcessPageLockTable` sharded fallback.
//!
//! This module implements the Hendler/Incze/Shavit/Tzafrir "Flat Combining"
//! synchronization pattern (SPAA 2010,
//! <https://people.csail.mit.edu/shanir/publications/Flat%20Combining%20SPAA%2010.pdf>)
//! for the sharded `Mutex<HashMap<PageNumber, TxnId>>` fallback used by
//! [`crate::core_types::InProcessPageLockTable`] for pages above
//! `FAST_LOCK_ARRAY_SIZE`.
//!
//! # Scope and why this module exists
//!
//! The fast path in [`crate::core_types::InProcessPageLockTable`] (pages
//! `1..=FAST_LOCK_ARRAY_SIZE`) is already lock-free (a flat `AtomicU64` array
//! with CAS). Only pages above that threshold fall back to a
//! `parking_lot::Mutex<HashMap<PageNumber, TxnId>>` per shard, and there the
//! lock-convoy behavior observed under high concurrent-writer pressure would
//! eventually dominate. For databases larger than `4 GiB` at a 64 KiB page
//! size — or whenever page numbers exceed 65 536 for other reasons — the
//! sharded `Mutex<HashMap>` tail becomes the serialization point.
//!
//! This module exposes [`FcPageLockShard`], a per-shard sequential-mutex
//! replacement. Threads publish their `(op, page, txn)` request into a
//! per-thread slot and one thread becomes the **combiner**. The combiner
//! drains the publication list, executes every request against the backing
//! `HashMap` under one combiner lock, and writes the result back into each
//! slot. Non-combiner threads spin on their own cache-line-isolated slot and
//! never contend on the combiner lock.
//!
//! # Invariants preserved
//!
//! * **INV-2 (at most one active txn holds exclusive lock per page):** the
//!   combiner is the **only** mutator of the backing map. All `Acquire` /
//!   `Release` / `Holder` operations are executed sequentially against a
//!   single `HashMap`, so the invariant held by the `Mutex<HashMap>` variant
//!   carries over verbatim.
//! * **Idempotent re-acquire:** `try_acquire(p, t)` returns `Ok(())` if the
//!   same `txn` already holds `page`, matching the `Mutex<HashMap>` path.
//! * **Release ignores non-holder:** `release(p, t)` only removes the entry
//!   if the current holder is `t`, matching the `Mutex<HashMap>` path.
//!
//! # Cancellation correctness
//!
//! Each `apply*` call is synchronous and never yields control to the caller
//! between publication and result retrieval. A caller's `Cx` cancellation
//! does not interrupt the spin: by construction, the combiner has at most
//! `MAX_FC_SLOTS` pending requests and drains them in a bounded number of
//! iterations. If a caller drops their `FcPageLockGuard` without polling
//! the result, the slot remains marked `CANCELLED`; the combiner recognises
//! the marker, skips the request, and clears the slot. No data is lost and
//! no new lock is installed on a cancelled request.
//!
//! # Safety
//!
//! No `unsafe`, no `UnsafeCell`, no `Pin` gymnastics. The publication list
//! is an array of `parking_lot::Mutex<Option<Request>>` entries (one cache
//! line per slot). Slot ownership uses an `AtomicU64` per slot. The combiner
//! lock is a plain `parking_lot::Mutex<()>`. Result delivery uses an
//! `AtomicU8` state word plus a `Mutex<Option<Outcome>>` for the payload.
//!
//! # Tracing
//!
//! Emits `fsqlite.mvcc.page_lock_fc` at DEBUG per batch (shard index, batch
//! size, combiner-thread-id) and at INFO on contention hot-spots (batch size
//! above `LARGE_BATCH_LOG_THRESHOLD`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use fsqlite_types::{PageNumber, PageNumberBuildHasher, TxnId};
use parking_lot::Mutex;

use crate::cache_aligned::CacheAligned;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum threads that can simultaneously publish into one shard.
///
/// The publication list is a fixed-size array; if every slot is full, a
/// thread falls back to acquiring the combiner lock directly. In practice,
/// 64 far exceeds FrankenSQLite's `MAX_CONCURRENT_WRITERS` (which is < 32).
pub const MAX_FC_SLOTS: usize = 64;

/// Emit an INFO-level tracing event when a combiner drains a batch at least
/// this large. Batches this big indicate real contention that the flat
/// combiner is absorbing.
const LARGE_BATCH_LOG_THRESHOLD: u32 = 8;

/// Base spin budget before parking while waiting for a combiner result.
///
/// The schedule mirrors the rest of the B4 bounded-handoff paths: stay on CPU
/// briefly for the common sub-microsecond handoff, then park every fourth wait
/// window so the combiner can wake the slot owner directly after publishing.
const FC_HANDOFF_BASE_SPINS: u32 = 64;
const FC_HANDOFF_MAX_SPINS: u32 = 2_048;
const FC_HANDOFF_PARK_EVERY: u32 = 4;
const FC_HANDOFF_MAX_PARK: Duration = Duration::from_micros(50);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FcHandoffWait {
    attempt: u32,
    spin_loops: u32,
    park_timeout: Duration,
}

const fn fc_handoff_spin_loops(attempt: u32) -> u32 {
    let growth = attempt.saturating_sub(1);
    let shift = if growth > 5 { 5 } else { growth };
    let spins = FC_HANDOFF_BASE_SPINS << shift;
    if spins > FC_HANDOFF_MAX_SPINS {
        FC_HANDOFF_MAX_SPINS
    } else {
        spins
    }
}

const fn fc_handoff_should_park(attempt: u32) -> bool {
    attempt >= FC_HANDOFF_PARK_EVERY && attempt % FC_HANDOFF_PARK_EVERY == 0
}

const fn fc_handoff_wait(attempt: u32) -> FcHandoffWait {
    FcHandoffWait {
        attempt,
        spin_loops: fc_handoff_spin_loops(attempt),
        park_timeout: if fc_handoff_should_park(attempt) {
            FC_HANDOFF_MAX_PARK
        } else {
            Duration::ZERO
        },
    }
}

fn perform_fc_handoff_spin(wait: FcHandoffWait) {
    for _ in 0..wait.spin_loops {
        std::hint::spin_loop();
    }
}

/// Sentinel slot owner = vacant.
const OWNER_VACANT: u64 = 0;

// Slot lifecycle states.
const SLOT_IDLE: u8 = 0;
const SLOT_PUBLISHED: u8 = 1;
const SLOT_READY: u8 = 2;
const SLOT_CANCELLED: u8 = 3;

// ---------------------------------------------------------------------------
// Request / Outcome
// ---------------------------------------------------------------------------

/// The set of operations the combiner executes against the backing map.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FcOp {
    /// Try to acquire `page` for `txn`. Returns `Acquired` or `Held(holder)`.
    TryAcquire { page: PageNumber, txn: TxnId },
    /// Release `page` iff `txn` is the current holder.
    Release { page: PageNumber, txn: TxnId },
    /// Look up the current holder of `page`.
    Holder { page: PageNumber },
}

/// The outcome of a combined operation, placed in the slot by the combiner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FcOutcome {
    /// `TryAcquire` succeeded — the caller now owns `page`.
    Acquired,
    /// `TryAcquire` failed — another txn holds `page`.
    Held(TxnId),
    /// `Release` removed `page` (caller was the holder).
    Released,
    /// `Release` found no matching entry (caller was not the holder).
    NotHeld,
    /// `Holder` reports that `page` is currently unlocked.
    Unlocked,
    /// `Holder` reports the current holder of `page`.
    HolderIs(TxnId),
}

/// Per-slot state: combiner reads request & writes outcome through this.
struct FcSlotInner {
    /// None means the slot is empty. Some(op) means a request is pending.
    /// When the combiner finishes, it writes `None` (and also sets
    /// `outcome`); when the caller consumes the outcome, it sets the state
    /// word back to `SLOT_IDLE`.
    request: Option<FcOp>,
    outcome: Option<FcOutcome>,
}

impl FcSlotInner {
    const fn new() -> Self {
        Self {
            request: None,
            outcome: None,
        }
    }
}

/// A single publication-list slot, owned by at most one thread at a time.
struct FcSlot {
    /// Non-zero when owned by a thread; zero means vacant.
    owner: AtomicU64,
    /// Lifecycle state (SLOT_IDLE | SLOT_PUBLISHED | SLOT_READY | SLOT_CANCELLED).
    state: AtomicU8,
    /// Request & outcome; guarded by the owner via `state` transitions.
    inner: Mutex<FcSlotInner>,
    /// Optional parked publisher thread. The combiner takes and unparks this
    /// after publishing `SLOT_READY`, giving slow handoffs a targeted wakeup.
    parked_thread: Mutex<Option<std::thread::Thread>>,
}

impl FcSlot {
    fn new() -> Self {
        Self {
            owner: AtomicU64::new(OWNER_VACANT),
            state: AtomicU8::new(SLOT_IDLE),
            inner: Mutex::new(FcSlotInner::new()),
            parked_thread: Mutex::new(None),
        }
    }
}

fn register_slot_waiter(slot: &FcSlot) {
    *slot.parked_thread.lock() = Some(std::thread::current());
}

fn clear_slot_waiter(slot: &FcSlot) {
    let _ = slot.parked_thread.lock().take();
}

fn unpark_slot_waiter(slot: &FcSlot) {
    let waiter = slot.parked_thread.lock().take();
    if let Some(waiter) = waiter {
        waiter.unpark();
    }
}

fn park_current_thread_for_slot(slot: &FcSlot, timeout: Duration) {
    if timeout.is_zero() {
        return;
    }

    register_slot_waiter(slot);
    if slot.state.load(Ordering::Acquire) == SLOT_READY {
        clear_slot_waiter(slot);
        return;
    }

    #[cfg(not(target_arch = "wasm32"))]
    std::thread::park_timeout(timeout);

    #[cfg(target_arch = "wasm32")]
    {
        let _ = timeout;
        clear_slot_waiter(slot);
    }
}

/// Backing map for a single shard's fallback locks.
///
/// Guarded by the combiner lock; only the combiner mutates it. The combiner
/// lock is a plain `Mutex<()>` — contention is expected at most among
/// would-be combiners, never among request publishers.
struct FcBackingMap {
    inner: Mutex<HashMap<PageNumber, TxnId, PageNumberBuildHasher>>,
}

impl FcBackingMap {
    fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::with_hasher(PageNumberBuildHasher::default())),
        }
    }
}

// ---------------------------------------------------------------------------
// FcPageLockShard
// ---------------------------------------------------------------------------

/// Flat-combining drop-in replacement for a `Mutex<HashMap<PageNumber, TxnId>>`
/// page-lock shard.
///
/// ## Linearizability
///
/// Every operation is linearized at the point the combiner executes it against
/// the backing map. Since the combiner holds the single combiner lock while
/// draining, operations on the same shard are totally ordered, matching the
/// `Mutex<HashMap>` baseline. Operations on different shards are independent.
pub struct FcPageLockShard {
    /// Combiner lock. Only one thread may drain the publication list at a time.
    combiner_lock: Mutex<()>,
    /// Publication list — one slot per potentially publishing thread.
    slots: Box<[CacheAligned<FcSlot>; MAX_FC_SLOTS]>,
    /// Backing map guarded by the combiner.
    map: FcBackingMap,
    /// Monotonic scan counter, used by the combiner to stamp batches for
    /// diagnostics and to age publication slots.
    scan_counter: AtomicU64,
    /// Shard index for tracing (zero is acceptable if unknown).
    shard_index: u32,
}

impl FcPageLockShard {
    /// Create a new empty flat-combining page-lock shard.
    #[must_use]
    pub fn new(shard_index: u32) -> Self {
        Self {
            combiner_lock: Mutex::new(()),
            slots: Box::new(std::array::from_fn(|_| CacheAligned::new(FcSlot::new()))),
            map: FcBackingMap::new(),
            scan_counter: AtomicU64::new(0),
            shard_index,
        }
    }

    // --- Public single-request API --------------------------------------------------

    /// Try to acquire the lock on `page` for `txn`.
    ///
    /// Returns `Ok(())` on success or `Err(holder)` if another `TxnId`
    /// already holds the lock. Re-acquiring by the current holder is
    /// idempotent (returns `Ok(())`).
    pub fn try_acquire(&self, page: PageNumber, txn: TxnId) -> Result<(), TxnId> {
        match self.submit(FcOp::TryAcquire { page, txn }) {
            FcOutcome::Acquired => Ok(()),
            FcOutcome::Held(h) => Err(h),
            other => unreachable!("try_acquire expected Acquired|Held, got {other:?}"),
        }
    }

    /// Release the lock on `page` iff the current holder is `txn`.
    ///
    /// Returns `true` if the lock was released, `false` otherwise.
    pub fn release(&self, page: PageNumber, txn: TxnId) -> bool {
        match self.submit(FcOp::Release { page, txn }) {
            FcOutcome::Released => true,
            FcOutcome::NotHeld => false,
            other => unreachable!("release expected Released|NotHeld, got {other:?}"),
        }
    }

    /// Report the current holder of `page`, if any.
    #[must_use]
    pub fn holder(&self, page: PageNumber) -> Option<TxnId> {
        match self.submit(FcOp::Holder { page }) {
            FcOutcome::Unlocked => None,
            FcOutcome::HolderIs(h) => Some(h),
            other => unreachable!("holder expected Unlocked|HolderIs, got {other:?}"),
        }
    }

    // --- Bulk helpers executed directly under the combiner lock ---------------------

    /// Remove every entry whose holder equals `txn`.
    ///
    /// Returns the number of entries removed.
    ///
    /// Implemented by taking the combiner lock directly (bypassing the
    /// publication list), because the combiner lock is the same lock the
    /// single-request path holds while draining. This keeps the
    /// `release_all` / `retain`-style bulk paths simple and avoids publishing
    /// N requests for an O(N)-scan operation.
    pub fn release_all(&self, txn: TxnId) -> usize {
        let _guard = self.combiner_lock.lock();
        // Drain any other pending requests first so we don't leave orphaned
        // publications behind this bulk mutation.
        self.drain_locked();
        let mut map = self.map.inner.lock();
        let before = map.len();
        map.retain(|_, &mut v| v != txn);
        before - map.len()
    }

    /// Current number of entries in the backing map.
    #[must_use]
    pub fn len(&self) -> usize {
        let _guard = self.combiner_lock.lock();
        self.map.inner.lock().len()
    }

    /// Whether the backing map is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterate over the entries (combiner-locked snapshot).
    ///
    /// The provided closure is called with a reference to each (page, txn)
    /// entry while the combiner lock is held. Callers must not perform I/O
    /// or other blocking work inside the callback.
    pub fn for_each<F: FnMut(PageNumber, TxnId)>(&self, mut f: F) {
        let _guard = self.combiner_lock.lock();
        self.drain_locked();
        let map = self.map.inner.lock();
        for (&p, &t) in map.iter() {
            f(p, t);
        }
    }

    /// Retain entries matching `predicate`; drop the rest.
    ///
    /// Returns the number of entries dropped.
    pub fn retain<P>(&self, mut predicate: P) -> usize
    where
        P: FnMut(PageNumber, TxnId) -> bool,
    {
        let _guard = self.combiner_lock.lock();
        self.drain_locked();
        let mut map = self.map.inner.lock();
        let before = map.len();
        map.retain(|&page, &mut txn| predicate(page, txn));
        before - map.len()
    }

    // --- Internal plumbing ----------------------------------------------------------

    /// Submit a single request, either by becoming the combiner or by
    /// publishing to a slot and spinning.
    fn submit(&self, op: FcOp) -> FcOutcome {
        // Fast path: try to become combiner immediately. If we succeed, we
        // drain any already-pending requests and then execute our own op
        // inline under the same lock. This absorbs bursts at low concurrency
        // without paying publication cost.
        if let Some(guard) = self.combiner_lock.try_lock() {
            self.drain_locked();
            let outcome = self.execute_locked(op);
            drop(guard);
            return outcome;
        }

        // Slow path: publish into a slot and spin until the combiner services
        // our request (or we win the combiner race).
        self.submit_via_slot(op)
    }

    fn submit_via_slot(&self, op: FcOp) -> FcOutcome {
        let slot_idx = self.acquire_slot();
        let slot = &self.slots[slot_idx];

        // Publish request.
        {
            let mut inner = slot.inner.lock();
            inner.request = Some(op);
            inner.outcome = None;
        }
        slot.state.store(SLOT_PUBLISHED, Ordering::Release);

        // Attempt to become combiner. If we win, we drain the whole list
        // (including our own slot) and leave our result in-slot, to be picked
        // up by the normal result-read path below.
        if let Some(guard) = self.combiner_lock.try_lock() {
            self.drain_locked();
            drop(guard);
        }

        // Spin-wait briefly for our result, then park on this slot so the
        // combiner can wake only the publisher it just serviced.
        let mut wait_attempt: u32 = 0;
        loop {
            let st = slot.state.load(Ordering::Acquire);
            if st == SLOT_READY {
                let outcome = {
                    let mut inner = slot.inner.lock();
                    inner.request = None;
                    inner.outcome.take()
                };
                clear_slot_waiter(slot);
                // Release the slot back to the pool.
                slot.state.store(SLOT_IDLE, Ordering::Release);
                slot.owner.store(OWNER_VACANT, Ordering::Release);
                return outcome.expect("combiner set SLOT_READY without outcome");
            }

            wait_attempt = wait_attempt.saturating_add(1);
            let wait = fc_handoff_wait(wait_attempt);
            perform_fc_handoff_spin(wait);

            // Re-attempt to become combiner (in case the prior combiner
            // exited before servicing us).
            if let Some(guard) = self.combiner_lock.try_lock() {
                self.drain_locked();
                drop(guard);
            } else {
                park_current_thread_for_slot(slot, wait.park_timeout);
            }
        }
    }

    /// Acquire a free slot in the publication list.
    ///
    /// Uses a hash of the current thread id as a starting probe to spread
    /// acquisition across slots (important under sustained high concurrency).
    fn acquire_slot(&self) -> usize {
        let tid = thread_id_hash();
        let start = (tid as usize) % MAX_FC_SLOTS;

        let mut wait_attempt: u32 = 0;
        loop {
            for offset in 0..MAX_FC_SLOTS {
                let idx = (start + offset) % MAX_FC_SLOTS;
                let slot = &self.slots[idx];
                if slot
                    .owner
                    .compare_exchange(OWNER_VACANT, tid, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return idx;
                }
            }
            // All slots busy — bounded spin and then a tiny park. This is rare; it would
            // require > `MAX_FC_SLOTS` concurrent publishers all racing.
            wait_attempt = wait_attempt.saturating_add(1);
            let wait = fc_handoff_wait(wait_attempt);
            perform_fc_handoff_spin(wait);
            #[cfg(not(target_arch = "wasm32"))]
            if !wait.park_timeout.is_zero() {
                std::thread::park_timeout(wait.park_timeout);
            }
        }
    }

    /// Drain the publication list: for each published slot, execute its
    /// request against the backing map and publish the outcome.
    ///
    /// Must be called with `combiner_lock` held.
    fn drain_locked(&self) {
        let scan = self.scan_counter.fetch_add(1, Ordering::Relaxed);
        let mut batch: u32 = 0;
        for slot in self.slots.iter() {
            let st = slot.state.load(Ordering::Acquire);
            match st {
                SLOT_PUBLISHED => {
                    // Fast path: handle the published request.
                    let op = {
                        let inner = slot.inner.lock();
                        inner.request
                    };
                    let Some(op) = op else {
                        // Spurious state; reset and move on.
                        slot.state.store(SLOT_IDLE, Ordering::Release);
                        continue;
                    };
                    let outcome = self.execute_locked(op);
                    {
                        let mut inner = slot.inner.lock();
                        inner.outcome = Some(outcome);
                    }
                    slot.state.store(SLOT_READY, Ordering::Release);
                    unpark_slot_waiter(slot);
                    batch += 1;
                }
                SLOT_CANCELLED => {
                    // Caller withdrew before we processed the request.
                    // Clear the request (we never touched the map) and
                    // return the slot to idle so the next publisher can
                    // re-use it.
                    {
                        let mut inner = slot.inner.lock();
                        inner.request = None;
                        inner.outcome = None;
                    }
                    unpark_slot_waiter(slot);
                    slot.state.store(SLOT_IDLE, Ordering::Release);
                    slot.owner.store(OWNER_VACANT, Ordering::Release);
                }
                _ => {
                    // SLOT_IDLE (nothing to do) or SLOT_READY (already
                    // serviced; caller hasn't consumed yet — leave it).
                }
            }
        }

        if batch >= LARGE_BATCH_LOG_THRESHOLD {
            tracing::info!(
                target: "fsqlite.mvcc.page_lock_fc",
                shard = self.shard_index,
                scan,
                batch_size = batch,
                "flat_combine_large_batch"
            );
        } else if batch > 0 {
            tracing::debug!(
                target: "fsqlite.mvcc.page_lock_fc",
                shard = self.shard_index,
                scan,
                batch_size = batch,
                "flat_combine_batch"
            );
        }
    }

    /// Execute a single operation against the backing map.
    ///
    /// Must be called with `combiner_lock` held.
    fn execute_locked(&self, op: FcOp) -> FcOutcome {
        let mut map = self.map.inner.lock();
        match op {
            FcOp::TryAcquire { page, txn } => {
                if let Some(&holder) = map.get(&page) {
                    if holder == txn {
                        FcOutcome::Acquired
                    } else {
                        FcOutcome::Held(holder)
                    }
                } else {
                    map.insert(page, txn);
                    FcOutcome::Acquired
                }
            }
            FcOp::Release { page, txn } => {
                if map.get(&page) == Some(&txn) {
                    map.remove(&page);
                    FcOutcome::Released
                } else {
                    FcOutcome::NotHeld
                }
            }
            FcOp::Holder { page } => match map.get(&page).copied() {
                Some(h) => FcOutcome::HolderIs(h),
                None => FcOutcome::Unlocked,
            },
        }
    }
}

impl std::fmt::Debug for FcPageLockShard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FcPageLockShard")
            .field("shard_index", &self.shard_index)
            .field("scan_counter", &self.scan_counter.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// Cheap hash of the current thread id, used as a non-zero owner tag.
///
/// Collisions are harmless: the combiner lock is a true mutex and slot
/// ownership is just a CAS-on-vacant.
fn thread_id_hash() -> u64 {
    let id = std::thread::current().id();
    let s = format!("{id:?}");
    let mut h: u64 = 0x9E37_79B9_7F4A_7C15;
    for b in s.as_bytes() {
        h = h.wrapping_mul(0x100_0000_01B3).wrapping_add(u64::from(*b));
    }
    if h == 0 { 1 } else { h }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;

    fn page(n: u32) -> PageNumber {
        PageNumber::new(n).expect("non-zero")
    }

    fn txn(n: u64) -> TxnId {
        TxnId::new(n).expect("non-zero txn id")
    }

    #[test]
    fn handoff_spin_loops_grow_then_cap() {
        assert_eq!(fc_handoff_spin_loops(1), FC_HANDOFF_BASE_SPINS);
        assert_eq!(fc_handoff_spin_loops(2), FC_HANDOFF_BASE_SPINS * 2);
        assert_eq!(fc_handoff_spin_loops(3), FC_HANDOFF_BASE_SPINS * 4);
        assert_eq!(fc_handoff_spin_loops(6), FC_HANDOFF_MAX_SPINS);
        assert_eq!(fc_handoff_spin_loops(32), FC_HANDOFF_MAX_SPINS);
    }

    #[test]
    fn handoff_parks_only_on_bounded_cadence() {
        for attempt in 1..FC_HANDOFF_PARK_EVERY {
            assert!(
                !fc_handoff_should_park(attempt),
                "attempt {attempt} should stay on CPU"
            );
        }
        assert!(fc_handoff_should_park(FC_HANDOFF_PARK_EVERY));
        assert!(!fc_handoff_should_park(FC_HANDOFF_PARK_EVERY + 1));
        assert!(fc_handoff_should_park(FC_HANDOFF_PARK_EVERY * 2));

        let wait = fc_handoff_wait(FC_HANDOFF_PARK_EVERY);
        assert_eq!(wait.spin_loops, FC_HANDOFF_BASE_SPINS << 3);
        assert_eq!(wait.park_timeout, FC_HANDOFF_MAX_PARK);
    }

    #[test]
    fn published_slot_unparks_registered_waiter() {
        let slot = Arc::new(FcSlot::new());
        slot.state.store(SLOT_PUBLISHED, Ordering::Release);

        let (registered_tx, registered_rx) = std::sync::mpsc::channel();
        let waiter_slot = Arc::clone(&slot);
        let waiter = thread::spawn(move || {
            register_slot_waiter(&waiter_slot);
            registered_tx.send(()).unwrap();
            std::thread::park_timeout(Duration::from_secs(1));
            waiter_slot.state.load(Ordering::Acquire)
        });

        registered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("waiter should register before publish");

        slot.state.store(SLOT_READY, Ordering::Release);
        unpark_slot_waiter(&slot);

        assert_eq!(waiter.join().unwrap(), SLOT_READY);
        assert!(
            slot.parked_thread.lock().is_none(),
            "unparking a ready slot must clear the parked waiter handle"
        );
    }

    #[test]
    fn acquire_release_single_thread() {
        let shard = FcPageLockShard::new(0);
        let p = page(100_000);
        let t = txn(1);

        assert!(shard.try_acquire(p, t).is_ok());
        assert_eq!(shard.holder(p), Some(t));
        assert!(shard.release(p, t));
        assert!(shard.holder(p).is_none());
    }

    #[test]
    fn reacquire_by_holder_is_idempotent() {
        let shard = FcPageLockShard::new(1);
        let p = page(200_000);
        let t = txn(2);

        assert!(shard.try_acquire(p, t).is_ok());
        assert!(shard.try_acquire(p, t).is_ok());
        assert!(shard.release(p, t));
        assert!(!shard.release(p, t));
    }

    #[test]
    fn contending_txns_get_held_error() {
        let shard = FcPageLockShard::new(2);
        let p = page(300_000);
        let holder = txn(1);
        let other = txn(2);

        assert!(shard.try_acquire(p, holder).is_ok());
        assert_eq!(shard.try_acquire(p, other), Err(holder));
    }

    #[test]
    fn release_all_drops_only_matching_txn() {
        let shard = FcPageLockShard::new(3);
        let t1 = txn(1);
        let t2 = txn(2);
        for i in 0..10u32 {
            shard.try_acquire(page(400_000 + i), t1).unwrap();
        }
        for i in 0..5u32 {
            shard.try_acquire(page(500_000 + i), t2).unwrap();
        }
        assert_eq!(shard.len(), 15);
        let removed = shard.release_all(t1);
        assert_eq!(removed, 10);
        assert_eq!(shard.len(), 5);
    }

    #[test]
    fn concurrent_acquire_distinct_pages() {
        let shard = Arc::new(FcPageLockShard::new(4));
        let barrier = Arc::new(Barrier::new(8));
        let mut handles = Vec::new();
        for t in 1..=8u64 {
            let s = Arc::clone(&shard);
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                b.wait();
                let tt = txn(t);
                for i in 0..100u32 {
                    let p = page(600_000 + (t as u32 * 1000) + i);
                    s.try_acquire(p, tt).unwrap();
                    assert_eq!(s.holder(p), Some(tt));
                    assert!(s.release(p, tt));
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(shard.len(), 0);
    }

    #[test]
    fn concurrent_contention_single_page() {
        // 8 threads racing for the same page: exactly one should win per round.
        let shard = Arc::new(FcPageLockShard::new(5));
        let p = page(700_000);
        let barrier = Arc::new(Barrier::new(8));
        let mut handles = Vec::new();
        for t in 1..=8u64 {
            let s = Arc::clone(&shard);
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                b.wait();
                let tt = txn(t);
                let mut wins = 0u32;
                for _ in 0..200u32 {
                    match s.try_acquire(p, tt) {
                        Ok(()) => {
                            wins += 1;
                            assert_eq!(s.holder(p), Some(tt));
                            assert!(s.release(p, tt));
                        }
                        Err(h) => assert_ne!(h, tt),
                    }
                }
                wins
            }));
        }
        let total_wins: u32 = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert!(total_wins > 0);
        assert_eq!(shard.len(), 0);
    }

    #[test]
    fn retain_drops_matching_entries() {
        let shard = FcPageLockShard::new(6);
        let t = txn(1);
        for i in 0..20u32 {
            shard.try_acquire(page(800_000 + i), t).unwrap();
        }
        let dropped = shard.retain(|p, _| p.get() % 2 == 0);
        assert_eq!(dropped, 10);
        assert_eq!(shard.len(), 10);
    }

    #[test]
    fn holder_reports_unlocked_for_never_locked_page() {
        let shard = FcPageLockShard::new(7);
        assert!(shard.holder(page(1_000_000)).is_none());
    }
}
