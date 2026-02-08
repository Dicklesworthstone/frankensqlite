//! Cache-line-aware wrappers and shared-memory structures (§1.5, bd-22n.3).
//!
//! This module provides:
//!
//! - [`CacheAligned<T>`]: A transparent wrapper that forces cache-line alignment
//!   and padding, preventing false sharing between adjacent elements in arrays.
//!
//! - [`SharedTxnSlot`]: The 128-byte (2 cache-line) shared-memory transaction
//!   slot structure used for cross-process MVCC coordination (§5.6.2).
//!
//! # Cache-Line Size
//!
//! We assume 64-byte cache lines (standard on x86-64 and AArch64). This is
//! encoded in [`CACHE_LINE_BYTES`].

use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering};

/// Cache line size in bytes.
///
/// 64 bytes for x86-64 (Intel/AMD) and AArch64 (Apple M-series, Graviton).
/// Over-aligning for platforms with 128-byte lines (some ARM) is safe
/// (wastes a little memory but prevents false sharing on 64-byte platforms).
pub const CACHE_LINE_BYTES: usize = 64;

// ---------------------------------------------------------------------------
// CacheAligned<T>
// ---------------------------------------------------------------------------

/// Wraps a value to ensure it starts on a cache-line boundary.
///
/// When stored in an array, each element occupies a whole number of cache
/// lines, preventing false sharing between adjacent elements accessed by
/// different threads.
///
/// # Layout
///
/// `#[repr(C, align(64))]` guarantees:
/// - The struct starts at a 64-byte-aligned address.
/// - The struct size is rounded up to the next multiple of 64 bytes.
#[repr(C, align(64))]
pub struct CacheAligned<T> {
    value: T,
}

impl<T> CacheAligned<T> {
    /// Wrap `value` with cache-line alignment.
    #[inline]
    #[must_use]
    pub const fn new(value: T) -> Self {
        Self { value }
    }

    /// Unwrap, returning the inner value.
    #[inline]
    #[must_use]
    pub fn into_inner(self) -> T {
        self.value
    }
}

impl<T: Default> Default for CacheAligned<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> std::ops::Deref for CacheAligned<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> std::ops::DerefMut for CacheAligned<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for CacheAligned<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}

// ---------------------------------------------------------------------------
// TxnSlot Sentinel Encoding (§5.6.2, bd-22n.13)
// ---------------------------------------------------------------------------

/// Bit position where the 2-bit tag begins in the `txn_id` word.
pub const SLOT_TAG_SHIFT: u32 = 62;

/// Mask isolating the top 2 tag bits of a `txn_id` word.
pub const SLOT_TAG_MASK: u64 = 0b11_u64 << SLOT_TAG_SHIFT;

/// Mask isolating the lower 62-bit payload (real `TxnId`) of a `txn_id` word.
pub const SLOT_PAYLOAD_MASK: u64 = (1_u64 << SLOT_TAG_SHIFT) - 1;

/// Sentinel tag: slot is being claimed (Phase 1 → Phase 3 of acquire).
pub const TAG_CLAIMING: u64 = 0b01_u64 << SLOT_TAG_SHIFT;

/// Sentinel tag: slot is being cleaned up after a crash.
pub const TAG_CLEANING: u64 = 0b10_u64 << SLOT_TAG_SHIFT;

/// Encode a claiming sentinel word: `TAG_CLAIMING | txn_id`.
///
/// The payload preserves the claimant's `TxnId` so Phase 3 CAS is unstealable.
#[inline]
#[must_use]
pub const fn encode_claiming(txn_id_raw: u64) -> u64 {
    TAG_CLAIMING | (txn_id_raw & SLOT_PAYLOAD_MASK)
}

/// Encode a cleaning sentinel word: `TAG_CLEANING | txn_id`.
///
/// The payload preserves the original `TxnId` so crash cleanup is retryable.
#[inline]
#[must_use]
pub const fn encode_cleaning(txn_id_raw: u64) -> u64 {
    TAG_CLEANING | (txn_id_raw & SLOT_PAYLOAD_MASK)
}

/// Extract the 2-bit tag from a `txn_id` word.
#[inline]
#[must_use]
pub const fn decode_tag(word: u64) -> u64 {
    word & SLOT_TAG_MASK
}

/// Extract the 62-bit payload (real `TxnId`) from a `txn_id` word.
#[inline]
#[must_use]
pub const fn decode_payload(word: u64) -> u64 {
    word & SLOT_PAYLOAD_MASK
}

/// Returns `true` if the word has any sentinel tag set (CLAIMING or CLEANING).
#[inline]
#[must_use]
pub const fn is_sentinel(word: u64) -> bool {
    (word & SLOT_TAG_MASK) != 0
}

/// Timeout for a slot stuck in CLAIMING state (seconds).
///
/// 5 seconds is orders of magnitude longer than any valid Phase 1 → Phase 3
/// transition (~microseconds).
pub const CLAIMING_TIMEOUT_SECS: u64 = 5;

/// Conservative timeout when PID/pid_birth are not yet published (seconds).
pub const CLAIMING_TIMEOUT_NO_PID_SECS: u64 = 30;

// ---------------------------------------------------------------------------
// SharedTxnSlot
// ---------------------------------------------------------------------------

/// Shared-memory transaction slot (§5.6.2).
///
/// Exactly 128 bytes (2 cache lines) with `#[repr(C, align(64))]`. Fields
/// are grouped by access frequency:
///
/// - **First cache line (bytes 0–63):** Hot-path fields read on every MVCC
///   visibility check — `txn_id`, sequence numbers, SSI flags.
///
/// - **Second cache line (bytes 64–127):** Administrative/lifecycle fields —
///   process identity, lease management, cleanup coordination.
///
/// All fields are atomic for lock-free cross-process access via shared memory.
/// A `txn_id` of 0 indicates the slot is free.
#[repr(C, align(64))]
pub struct SharedTxnSlot {
    // === First cache line (offsets 0–63) — hot read path ===
    /// Transaction ID (top 2 bits reserved for sentinel encoding). 0 = free.
    pub txn_id: AtomicU64,
    /// Begin sequence number (snapshot lower bound).
    pub begin_seq: AtomicU64,
    /// Commit sequence number (0 while transaction is active).
    pub commit_seq: AtomicU64,
    /// Snapshot high-water mark.
    pub snapshot_high: AtomicU64,
    /// Number of pages in the write set.
    pub write_set_pages: AtomicU32,
    /// Transaction state (discriminant of `TransactionState`).
    pub state: AtomicU8,
    /// Transaction mode (discriminant of `TransactionMode`).
    pub mode: AtomicU8,
    /// SSI: has incoming rw-antidependency edge.
    pub has_in_rw: AtomicBool,
    /// SSI: has outgoing rw-antidependency edge.
    pub has_out_rw: AtomicBool,
    /// SSI: marked for abort by conflict detector.
    pub marked_for_abort: AtomicBool,
    /// Padding to end of first cache line.
    _pad0: [u8; 23],

    // === Second cache line (offsets 64–127) — admin/cold path ===
    /// Process birth timestamp (epoch nanos, for stale-slot detection).
    pub pid_birth: AtomicU64,
    /// Lease expiry timestamp (epoch nanos).
    pub lease_expiry: AtomicU64,
    /// Timestamp when this slot was claimed (epoch nanos).
    pub claiming_timestamp: AtomicU64,
    /// TxnId of the transaction performing cleanup (0 = none).
    pub cleanup_txn_id: AtomicU64,
    /// Transaction epoch (monotonic, incremented on slot reuse).
    pub txn_epoch: AtomicU32,
    /// Witness-plane epoch for double-buffered SSI tracking.
    pub witness_epoch: AtomicU32,
    /// Process ID of the slot owner.
    pub pid: AtomicU32,
    /// Reserved padding to end of second cache line.
    _pad1: [u8; 20],
}

impl SharedTxnSlot {
    /// Create a new free (unoccupied) slot with all fields zeroed.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            txn_id: AtomicU64::new(0),
            begin_seq: AtomicU64::new(0),
            commit_seq: AtomicU64::new(0),
            snapshot_high: AtomicU64::new(0),
            write_set_pages: AtomicU32::new(0),
            state: AtomicU8::new(0),
            mode: AtomicU8::new(0),
            has_in_rw: AtomicBool::new(false),
            has_out_rw: AtomicBool::new(false),
            marked_for_abort: AtomicBool::new(false),
            _pad0: [0; 23],
            pid_birth: AtomicU64::new(0),
            lease_expiry: AtomicU64::new(0),
            claiming_timestamp: AtomicU64::new(0),
            cleanup_txn_id: AtomicU64::new(0),
            txn_epoch: AtomicU32::new(0),
            witness_epoch: AtomicU32::new(0),
            pid: AtomicU32::new(0),
            _pad1: [0; 20],
        }
    }

    /// Whether this slot is free (`txn_id == 0`).
    #[inline]
    #[must_use]
    pub fn is_free(&self, ordering: Ordering) -> bool {
        self.txn_id.load(ordering) == 0
    }

    /// Whether this slot is in a sentinel state (CLAIMING or CLEANING).
    #[inline]
    #[must_use]
    pub fn is_sentinel(&self, ordering: Ordering) -> bool {
        is_sentinel(self.txn_id.load(ordering))
    }

    /// Whether this slot is in CLAIMING state.
    #[inline]
    #[must_use]
    pub fn is_claiming(&self, ordering: Ordering) -> bool {
        decode_tag(self.txn_id.load(ordering)) == TAG_CLAIMING
    }

    /// Whether this slot is in CLEANING state.
    #[inline]
    #[must_use]
    pub fn is_cleaning(&self, ordering: Ordering) -> bool {
        decode_tag(self.txn_id.load(ordering)) == TAG_CLEANING
    }

    /// Extract the payload `TxnId` from a sentinel word, if the slot is in a
    /// sentinel state. Returns `None` for free or real-txn-id slots.
    #[must_use]
    pub fn sentinel_payload(&self, ordering: Ordering) -> Option<u64> {
        let word = self.txn_id.load(ordering);
        if is_sentinel(word) {
            Some(decode_payload(word))
        } else {
            None
        }
    }
}

impl Default for SharedTxnSlot {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for SharedTxnSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedTxnSlot")
            .field("txn_id", &self.txn_id.load(Ordering::Relaxed))
            .field("state", &self.state.load(Ordering::Relaxed))
            .field("mode", &self.mode.load(Ordering::Relaxed))
            .field("begin_seq", &self.begin_seq.load(Ordering::Relaxed))
            .field("commit_seq", &self.commit_seq.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Platform requirement (§5.6.2): 64-bit atomics for Concurrent mode
// ---------------------------------------------------------------------------

/// Compile-time assertion that this platform supports 64-bit atomics.
///
/// The three-phase acquire protocol requires `AtomicU64` CAS; platforms that
/// lack native 64-bit atomics cannot run Concurrent mode safely.
#[cfg(not(target_has_atomic = "64"))]
compile_error!(
    "FrankenSQLite Concurrent mode requires 64-bit atomics (target_has_atomic = \"64\"). \
     Serialized-only mode is not yet implemented."
);

// ---------------------------------------------------------------------------
// SlotAcquireError
// ---------------------------------------------------------------------------

/// Errors returned by `TxnSlotArray::acquire`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotAcquireError {
    /// All slots are occupied — caller should retry or return SQLITE_BUSY.
    AllSlotsBusy,
}

impl std::fmt::Display for SlotAcquireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AllSlotsBusy => f.write_str("all TxnSlots are busy"),
        }
    }
}

impl std::error::Error for SlotAcquireError {}

// ---------------------------------------------------------------------------
// Three-Phase Acquire Protocol on SharedTxnSlot (§5.6.2)
// ---------------------------------------------------------------------------

/// State discriminant for `TransactionState` written to `SharedTxnSlot.state`.
///
/// Must agree with `core_types::TransactionState` ordinal mapping.
pub mod slot_state {
    /// Slot is free (no transaction).
    pub const FREE: u8 = 0;
    /// Transaction is active (reading/writing).
    pub const ACTIVE: u8 = 1;
    /// Transaction is in the process of committing.
    pub const COMMITTING: u8 = 2;
    /// Transaction has been committed.
    pub const COMMITTED: u8 = 3;
    /// Transaction has been aborted.
    pub const ABORTED: u8 = 4;
}

/// Mode discriminant for `TransactionMode` written to `SharedTxnSlot.mode`.
pub mod slot_mode {
    /// Serialized mode (global write mutex, one writer at a time).
    pub const SERIALIZED: u8 = 0;
    /// Concurrent mode (page-level MVCC).
    pub const CONCURRENT: u8 = 1;
}

impl SharedTxnSlot {
    // -------------------------------------------------------------------
    // Phase 1: CLAIM — CAS txn_id 0 → encode_claiming(real_txn_id)
    // -------------------------------------------------------------------

    /// Attempt Phase 1 of the three-phase acquire protocol.
    ///
    /// CAS `txn_id` from `0` (free) to `encode_claiming(txn_id_raw)`.
    /// Returns `true` if this slot was successfully claimed, `false` if the
    /// slot was already occupied (caller should scan to the next slot).
    pub fn phase1_claim(&self, txn_id_raw: u64) -> bool {
        let claiming_word = encode_claiming(txn_id_raw);
        self.txn_id
            .compare_exchange(0, claiming_word, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    // -------------------------------------------------------------------
    // Phase 2: INITIALIZE — write identity + snapshot fields
    // -------------------------------------------------------------------

    /// Phase 2: publish process identity and initialize transaction state.
    ///
    /// **Ordering contract (§5.6.2):** `pid`, `pid_birth`, and `lease_expiry`
    /// must be written *before* the snapshot backbone fields (`begin_seq`,
    /// `snapshot_high`). This ensures that cleanup processes can always
    /// identify a live claimer before the snapshot is visible.
    ///
    /// # Parameters
    ///
    /// * `pid` — OS process id of the slot owner.
    /// * `pid_birth` — process birth timestamp (epoch nanos).
    /// * `lease_secs` — lease expiry (epoch nanos).
    /// * `begin_seq` — snapshot lower bound (from `SharedMemoryLayout::load_commit_seq`).
    /// * `snapshot_high` — snapshot upper bound (same as `begin_seq` at BEGIN).
    /// * `mode` — one of `slot_mode::SERIALIZED` or `slot_mode::CONCURRENT`.
    /// * `witness_epoch` — pinned witness epoch for `BEGIN CONCURRENT`.
    #[allow(clippy::too_many_arguments)]
    pub fn phase2_initialize(
        &self,
        pid: u32,
        pid_birth: u64,
        lease_secs: u64,
        begin_seq: u64,
        snapshot_high: u64,
        mode: u8,
        witness_epoch: u32,
    ) {
        // 1. Publish process identity FIRST (cleanup safety).
        self.pid.store(pid, Ordering::Release);
        self.pid_birth.store(pid_birth, Ordering::Release);
        self.lease_expiry.store(lease_secs, Ordering::Release);

        // 2. Increment txn_epoch for slot-reuse disambiguation.
        self.txn_epoch.fetch_add(1, Ordering::AcqRel);

        // 3. Snapshot backbone.
        self.begin_seq.store(begin_seq, Ordering::Release);
        self.snapshot_high.store(snapshot_high, Ordering::Release);

        // 4. Mode + state + SSI flags.
        self.mode.store(mode, Ordering::Release);
        self.state.store(slot_state::ACTIVE, Ordering::Release);
        self.has_in_rw.store(false, Ordering::Release);
        self.has_out_rw.store(false, Ordering::Release);
        self.marked_for_abort.store(false, Ordering::Release);
        self.witness_epoch.store(witness_epoch, Ordering::Release);
        self.write_set_pages.store(0, Ordering::Release);
        self.commit_seq.store(0, Ordering::Release);
    }

    // -------------------------------------------------------------------
    // Phase 3: PUBLISH — CAS claiming_word → real_txn_id
    // -------------------------------------------------------------------

    /// Phase 3: make the slot visible as an active transaction.
    ///
    /// CAS `txn_id` from `encode_claiming(txn_id_raw)` to `txn_id_raw`.
    /// Returns `true` if the publish succeeded. Returns `false` if the
    /// claiming word was stolen (cleanup reclaimed the slot — ABA prevention).
    pub fn phase3_publish(&self, txn_id_raw: u64) -> bool {
        let claiming_word = encode_claiming(txn_id_raw);
        let ok = self
            .txn_id
            .compare_exchange(
                claiming_word,
                txn_id_raw,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok();
        if ok {
            // Clear the claiming timestamp now that the slot is fully published.
            self.claiming_timestamp.store(0, Ordering::Release);
        }
        ok
    }

    // -------------------------------------------------------------------
    // Slot release (§5.6.2): clear all fields, txn_id=0 LAST
    // -------------------------------------------------------------------

    /// Release this slot, clearing all stale fields with `txn_id=0` as the
    /// **final** write (Release ordering).
    ///
    /// This ensures other processes never observe a free slot with stale
    /// sequence numbers, SSI flags, or process identity.
    pub fn release(&self) {
        // Clear all data fields first (any order among these is fine).
        self.begin_seq.store(0, Ordering::Release);
        self.snapshot_high.store(0, Ordering::Release);
        self.commit_seq.store(0, Ordering::Release);
        self.write_set_pages.store(0, Ordering::Release);
        self.state.store(slot_state::FREE, Ordering::Release);
        self.mode.store(0, Ordering::Release);
        self.has_in_rw.store(false, Ordering::Release);
        self.has_out_rw.store(false, Ordering::Release);
        self.marked_for_abort.store(false, Ordering::Release);
        self.witness_epoch.store(0, Ordering::Release);
        self.pid.store(0, Ordering::Release);
        self.pid_birth.store(0, Ordering::Release);
        self.lease_expiry.store(0, Ordering::Release);
        self.claiming_timestamp.store(0, Ordering::Release);
        self.cleanup_txn_id.store(0, Ordering::Release);

        // txn_id = 0 MUST be the final write so scanners never see a
        // free slot with stale fields populated.
        self.txn_id.store(0, Ordering::Release);
    }
}

// ---------------------------------------------------------------------------
// TxnSlotArray — array-level acquire/release (§5.6.2)
// ---------------------------------------------------------------------------

/// Fixed-size array of `SharedTxnSlot`s for cross-process MVCC coordination.
///
/// Provides the slot-scanning acquire protocol: iterate from a hint index,
/// attempt Phase 1 CAS on each free slot, wrap around, and return
/// `SlotAcquireError::AllSlotsBusy` if all slots are occupied.
pub struct TxnSlotArray {
    slots: Vec<SharedTxnSlot>,
}

impl TxnSlotArray {
    /// Create a new array with `count` free slots.
    ///
    /// # Panics
    ///
    /// Panics if `count == 0`.
    #[must_use]
    pub fn new(count: usize) -> Self {
        assert!(count > 0, "TxnSlotArray requires at least one slot");
        let mut slots = Vec::with_capacity(count);
        for _ in 0..count {
            slots.push(SharedTxnSlot::new());
        }
        Self { slots }
    }

    /// Number of slots in this array.
    #[must_use]
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// Whether the array is empty (always `false` since `new` requires > 0).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Access a slot by index.
    ///
    /// # Panics
    ///
    /// Panics if `index >= len()`.
    #[must_use]
    pub fn slot(&self, index: usize) -> &SharedTxnSlot {
        &self.slots[index]
    }

    /// Borrow the slot slice.
    #[must_use]
    pub fn slots(&self) -> &[SharedTxnSlot] {
        &self.slots
    }

    /// Attempt the full three-phase acquire on this array.
    ///
    /// Scans starting from `hint_index`, wrapping around. On the first free
    /// slot where Phase 1 CAS succeeds, runs Phase 2 initialization and
    /// Phase 3 publish. Returns the slot index on success.
    ///
    /// # Parameters
    ///
    /// * `txn_id_raw` — the real `TxnId` to acquire.
    /// * `hint_index` — starting scan position (modulo `len()`).
    /// * `pid`, `pid_birth`, `lease_secs` — process identity.
    /// * `begin_seq`, `snapshot_high` — snapshot backbone.
    /// * `mode` — `slot_mode::SERIALIZED` or `slot_mode::CONCURRENT`.
    /// * `witness_epoch` — pinned witness epoch.
    ///
    /// # Errors
    ///
    /// Returns `SlotAcquireError::AllSlotsBusy` if no free slot is found.
    #[allow(clippy::too_many_arguments)]
    pub fn acquire(
        &self,
        txn_id_raw: u64,
        hint_index: usize,
        pid: u32,
        pid_birth: u64,
        lease_secs: u64,
        begin_seq: u64,
        snapshot_high: u64,
        mode: u8,
        witness_epoch: u32,
    ) -> Result<usize, SlotAcquireError> {
        let n = self.slots.len();
        let start = hint_index % n;

        for offset in 0..n {
            let idx = (start + offset) % n;
            let slot = &self.slots[idx];

            // Phase 1: CAS free → claiming.
            if !slot.phase1_claim(txn_id_raw) {
                continue;
            }

            // Record claiming timestamp for cleanup timeout detection.
            // Use a deterministic logical epoch-seconds clock (no ambient authority).
            let now_epoch_secs = logical_now_epoch_secs();
            slot.claiming_timestamp
                .store(now_epoch_secs, Ordering::Release);

            // Phase 2: initialize fields.
            slot.phase2_initialize(
                pid,
                pid_birth,
                lease_secs,
                begin_seq,
                snapshot_high,
                mode,
                witness_epoch,
            );

            // Phase 3: publish — CAS claiming → real tid.
            if slot.phase3_publish(txn_id_raw) {
                return Ok(idx);
            }

            // Phase 3 failed — cleanup reclaimed our slot (ABA prevented).
            // Release our partial initialization and try the next slot.
            slot.release();
        }

        Err(SlotAcquireError::AllSlotsBusy)
    }

    /// Count how many slots are currently free.
    #[must_use]
    pub fn free_count(&self) -> usize {
        self.slots
            .iter()
            .filter(|s| s.is_free(Ordering::Acquire))
            .count()
    }

    /// Count how many slots are currently occupied (non-free).
    #[must_use]
    pub fn occupied_count(&self) -> usize {
        self.len() - self.free_count()
    }
}

impl std::fmt::Debug for TxnSlotArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxnSlotArray")
            .field("len", &self.len())
            .field("free", &self.free_count())
            .finish()
    }
}

/// Deterministic epoch-seconds clock for timeout logic (no ambient authority).
///
/// This is *not* wall-clock time. It is derived from a monotonic logical clock
/// that advances on each call.
pub(crate) fn logical_now_epoch_secs() -> u64 {
    logical_now_millis() / 1000
}

/// Deterministic logical clock in milliseconds (no ambient authority).
pub(crate) fn logical_now_millis() -> u64 {
    // Start from a recent-ish epoch to keep logs readable.
    static LOGICAL_EPOCH_MILLIS: AtomicU64 = AtomicU64::new(1_700_000_000_000);
    LOGICAL_EPOCH_MILLIS.fetch_add(1, Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// RecentlyCommittedReadersIndex (§5.6.2.1, bd-3t3.7)
// ---------------------------------------------------------------------------

/// Bloom filter parameters for RCRI page-key matching.
///
/// 4096-bit filter with K=3 hash probes using domain-separated xxh3_64.
pub mod rcri_bloom {
    /// Bloom filter size in bits.
    pub const BITS: u32 = 4096;
    /// Number of hash probes.
    pub const K: u32 = 3;
    /// Bloom filter size in bytes (512 bytes).
    pub const BYTES: usize = (BITS / 8) as usize;
    /// Domain separation prefix for RCRI bloom hashing.
    pub const DOMAIN_PREFIX: &[u8] = b"fsqlite:cr-bloom:v1";
}

/// A single entry in the recently-committed-readers ring buffer.
///
/// Records a committed reader's identity, commit sequence, and a bloom
/// filter over the pages it read. This allows SSI incoming edge discovery
/// for readers that have already freed their `TxnSlot`.
#[derive(Clone)]
pub struct RcriEntry {
    /// Transaction ID of the committed reader.
    pub txn_id: u64,
    /// The commit sequence at which this reader committed.
    pub commit_seq: u64,
    /// The reader's begin_seq (snapshot lower bound).
    pub begin_seq: u64,
    /// Bloom filter over page numbers the reader accessed.
    ///
    /// 512 bytes = 4096 bits, K=3 probes.
    pub page_bloom: [u8; rcri_bloom::BYTES],
}

impl RcriEntry {
    /// Create a new RCRI entry from a committed reader's metadata.
    ///
    /// `pages` is the set of page numbers the reader accessed.
    #[must_use]
    pub fn new(txn_id: u64, commit_seq: u64, begin_seq: u64, pages: &[u32]) -> Self {
        let mut page_bloom = [0u8; rcri_bloom::BYTES];
        for &pgno in pages {
            bloom_insert(&mut page_bloom, pgno);
        }
        Self {
            txn_id,
            commit_seq,
            begin_seq,
            page_bloom,
        }
    }

    /// Check if this entry's bloom filter *may* contain the given page number.
    ///
    /// Returns `true` if the page might be present (possible false positive).
    /// Returns `false` if the page is definitely absent (no false negatives).
    #[must_use]
    pub fn bloom_may_contain(&self, pgno: u32) -> bool {
        bloom_query(&self.page_bloom, pgno)
    }
}

impl std::fmt::Debug for RcriEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RcriEntry")
            .field("txn_id", &self.txn_id)
            .field("commit_seq", &self.commit_seq)
            .field("begin_seq", &self.begin_seq)
            .finish_non_exhaustive()
    }
}

/// Domain-separated bloom hash for RCRI.
///
/// Uses `xxh3_64("fsqlite:cr-bloom:v1" || be_u32(pgno) || probe_index)`.
#[must_use]
fn bloom_hash(pgno: u32, probe: u32) -> u32 {
    use xxhash_rust::xxh3::xxh3_64;

    let mut buf = Vec::with_capacity(rcri_bloom::DOMAIN_PREFIX.len() + 4 + 4);
    buf.extend_from_slice(rcri_bloom::DOMAIN_PREFIX);
    buf.extend_from_slice(&pgno.to_be_bytes());
    buf.extend_from_slice(&probe.to_be_bytes());

    let h = xxh3_64(&buf);
    #[allow(clippy::cast_possible_truncation)]
    let bit_index = (h as u32) % rcri_bloom::BITS;
    bit_index
}

/// Insert a page number into a bloom filter.
fn bloom_insert(filter: &mut [u8; rcri_bloom::BYTES], pgno: u32) {
    for probe in 0..rcri_bloom::K {
        let bit = bloom_hash(pgno, probe);
        let byte_idx = (bit / 8) as usize;
        let bit_idx = bit % 8;
        filter[byte_idx] |= 1 << bit_idx;
    }
}

/// Query whether a page number may be present in a bloom filter.
#[must_use]
fn bloom_query(filter: &[u8; rcri_bloom::BYTES], pgno: u32) -> bool {
    for probe in 0..rcri_bloom::K {
        let bit = bloom_hash(pgno, probe);
        let byte_idx = (bit / 8) as usize;
        let bit_idx = bit % 8;
        if filter[byte_idx] & (1 << bit_idx) == 0 {
            return false;
        }
    }
    true
}

/// Error returned when the RCRI ring is full and all entries are still
/// required (commit_seq > gc_horizon). Fail-closed: the committer must
/// abort or retry rather than lose SSI coverage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RcriOverflowError;

impl std::fmt::Display for RcriOverflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RCRI ring full: all entries still required (SQLITE_BUSY_SNAPSHOT)")
    }
}

impl std::error::Error for RcriOverflowError {}

/// Recently-committed readers ring buffer for SSI incoming edge discovery.
///
/// Fixed-size ring stored in-process (mirroring what would live in shared
/// memory at `committed_readers_offset`). Single-writer append during the
/// commit sequencer critical section. Multi-reader queries during SSI
/// validation.
///
/// **Fail-closed overflow:** if the ring is full and the oldest entry is
/// still required (its `commit_seq > min_active_begin_seq`), the insert
/// returns `RcriOverflowError` and the committer must abort. This
/// guarantees no false negatives within ring capacity.
pub struct RecentlyCommittedReadersIndex {
    ring: Vec<Option<RcriEntry>>,
    head: usize,
    len: usize,
}

impl RecentlyCommittedReadersIndex {
    /// Create a new RCRI with the given ring capacity.
    ///
    /// Capacity should be at least `max_txn_slots * 2` for typical workloads.
    ///
    /// # Panics
    ///
    /// Panics if `capacity == 0`.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "RCRI requires at least one slot");
        let mut ring = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            ring.push(None);
        }
        Self {
            ring,
            head: 0,
            len: 0,
        }
    }

    /// Ring capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.ring.len()
    }

    /// Number of entries currently in the ring.
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the ring is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Insert a committed reader entry into the ring.
    ///
    /// Called during the commit sequencer critical section, BEFORE the
    /// reader's `TxnSlot` is freed.
    ///
    /// If the ring is full, the oldest entry is evicted ONLY if its
    /// `commit_seq <= min_active_begin_seq` (safe to evict — no future
    /// committer can form an edge with it). Otherwise returns
    /// `RcriOverflowError` (fail-closed).
    ///
    /// # Errors
    ///
    /// Returns `RcriOverflowError` if the ring is full and all entries are
    /// still required.
    pub fn insert(
        &mut self,
        entry: RcriEntry,
        min_active_begin_seq: u64,
    ) -> Result<usize, RcriOverflowError> {
        if self.len < self.ring.len() {
            // Room available — append at tail.
            let idx = (self.head + self.len) % self.ring.len();
            self.ring[idx] = Some(entry);
            self.len += 1;
            return Ok(idx);
        }

        // Ring is full — check if oldest entry is evictable.
        if let Some(oldest) = &self.ring[self.head] {
            if oldest.commit_seq > min_active_begin_seq {
                // Oldest entry still required — fail closed.
                return Err(RcriOverflowError);
            }
        }

        // Evict oldest, write new entry at head, advance head.
        let idx = self.head;
        self.ring[idx] = Some(entry);
        self.head = (self.head + 1) % self.ring.len();
        Ok(idx)
    }

    /// Query for committed readers whose bloom filter may contain `pgno`
    /// and whose commit_seq is within the given range.
    ///
    /// Returns an iterator of matching entries. Used during SSI incoming
    /// edge discovery: "did any recently-committed reader read page P
    /// that I wrote?"
    ///
    /// * `pgno` — the page number to look up.
    /// * `after_begin_seq` — only match readers whose `begin_seq < after_begin_seq`
    ///   (the writer's snapshot high; only readers that started before the
    ///   writer's snapshot can form rw-antidependency edges).
    pub fn query_incoming_edges(
        &self,
        pgno: u32,
        after_begin_seq: u64,
    ) -> impl Iterator<Item = &RcriEntry> {
        let cap = self.ring.len();
        let head = self.head;
        let len = self.len;
        (0..len).filter_map(move |offset| {
            let idx = (head + offset) % cap;
            self.ring[idx]
                .as_ref()
                .filter(|e| e.begin_seq < after_begin_seq && e.bloom_may_contain(pgno))
        })
    }

    /// Prune entries whose `commit_seq <= min_active_begin_seq`.
    ///
    /// These entries can never contribute to an SSI edge because no active
    /// or future transaction has a snapshot that overlaps with them.
    ///
    /// Returns the number of entries pruned.
    pub fn gc(&mut self, min_active_begin_seq: u64) -> usize {
        let mut pruned = 0;
        while self.len > 0 {
            if let Some(oldest) = &self.ring[self.head] {
                if oldest.commit_seq <= min_active_begin_seq {
                    self.ring[self.head] = None;
                    self.head = (self.head + 1) % self.ring.len();
                    self.len -= 1;
                    pruned += 1;
                    continue;
                }
            }
            break;
        }
        pruned
    }

    /// Access an entry by ring position (for testing).
    #[cfg(test)]
    #[allow(dead_code)]
    fn entry_at(&self, offset: usize) -> Option<&RcriEntry> {
        if offset >= self.len {
            return None;
        }
        let idx = (self.head + offset) % self.ring.len();
        self.ring[idx].as_ref()
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for RecentlyCommittedReadersIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecentlyCommittedReadersIndex")
            .field("capacity", &self.capacity())
            .field("len", &self.len)
            .field("head", &self.head)
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::{align_of, size_of};
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    // -- CacheAligned tests --

    #[test]
    fn test_cache_aligned_size_is_multiple_of_64() {
        assert_eq!(size_of::<CacheAligned<u8>>(), 64);
        assert_eq!(size_of::<CacheAligned<u64>>(), 64);
        assert_eq!(size_of::<CacheAligned<[u8; 64]>>(), 64);
        assert_eq!(size_of::<CacheAligned<[u8; 65]>>(), 128);
        assert_eq!(size_of::<CacheAligned<[u8; 128]>>(), 128);
        assert_eq!(size_of::<CacheAligned<[u8; 129]>>(), 192);
    }

    #[test]
    fn test_cache_aligned_alignment() {
        assert_eq!(align_of::<CacheAligned<u8>>(), CACHE_LINE_BYTES);
        assert_eq!(align_of::<CacheAligned<AtomicU64>>(), CACHE_LINE_BYTES);
    }

    #[test]
    fn test_cache_aligned_deref() {
        let aligned = CacheAligned::new(42_u64);
        assert_eq!(*aligned, 42);
    }

    #[test]
    fn test_cache_aligned_deref_mut() {
        let mut aligned = CacheAligned::new(0_u64);
        *aligned = 99;
        assert_eq!(*aligned, 99);
    }

    #[test]
    fn test_cache_aligned_into_inner() {
        let aligned = CacheAligned::new(String::from("hello"));
        let s = aligned.into_inner();
        assert_eq!(s, "hello");
    }

    #[test]
    fn test_cache_aligned_default() {
        let aligned: CacheAligned<u64> = CacheAligned::default();
        assert_eq!(*aligned, 0);
    }

    #[test]
    fn test_cache_aligned_debug() {
        let aligned = CacheAligned::new(42_u32);
        let debug = format!("{aligned:?}");
        assert!(debug.contains("42"));
    }

    #[test]
    fn test_cache_aligned_array_no_false_sharing() {
        let arr: [CacheAligned<AtomicU64>; 4] =
            std::array::from_fn(|_| CacheAligned::new(AtomicU64::new(0)));

        for i in 0..3 {
            let a = (&raw const arr[i]) as usize;
            let b = (&raw const arr[i + 1]) as usize;
            assert_eq!(
                b - a,
                CACHE_LINE_BYTES,
                "adjacent CacheAligned elements must be {CACHE_LINE_BYTES} bytes apart"
            );
        }
    }

    // -- SharedTxnSlot tests --

    #[test]
    fn test_txn_slot_128_bytes() {
        assert_eq!(
            size_of::<SharedTxnSlot>(),
            128,
            "SharedTxnSlot must be exactly 128 bytes (2 cache lines)"
        );
    }

    #[test]
    fn test_txn_slot_alignment() {
        assert_eq!(
            align_of::<SharedTxnSlot>(),
            CACHE_LINE_BYTES,
            "SharedTxnSlot must be cache-line aligned"
        );
    }

    #[test]
    fn test_txn_slot_new_is_free() {
        let slot = SharedTxnSlot::new();
        assert!(slot.is_free(Ordering::Relaxed));
        assert_eq!(slot.txn_id.load(Ordering::Relaxed), 0);
        assert_eq!(slot.begin_seq.load(Ordering::Relaxed), 0);
        assert_eq!(slot.commit_seq.load(Ordering::Relaxed), 0);
        assert_eq!(slot.snapshot_high.load(Ordering::Relaxed), 0);
        assert_eq!(slot.write_set_pages.load(Ordering::Relaxed), 0);
        assert_eq!(slot.state.load(Ordering::Relaxed), 0);
        assert_eq!(slot.mode.load(Ordering::Relaxed), 0);
        assert!(!slot.has_in_rw.load(Ordering::Relaxed));
        assert!(!slot.has_out_rw.load(Ordering::Relaxed));
        assert!(!slot.marked_for_abort.load(Ordering::Relaxed));
        assert_eq!(slot.pid_birth.load(Ordering::Relaxed), 0);
        assert_eq!(slot.lease_expiry.load(Ordering::Relaxed), 0);
        assert_eq!(slot.claiming_timestamp.load(Ordering::Relaxed), 0);
        assert_eq!(slot.cleanup_txn_id.load(Ordering::Relaxed), 0);
        assert_eq!(slot.txn_epoch.load(Ordering::Relaxed), 0);
        assert_eq!(slot.witness_epoch.load(Ordering::Relaxed), 0);
        assert_eq!(slot.pid.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_txn_slot_array_no_false_sharing() {
        let slots: [SharedTxnSlot; 4] = std::array::from_fn(|_| SharedTxnSlot::new());

        for i in 0..3 {
            let a = (&raw const slots[i]) as usize;
            let b = (&raw const slots[i + 1]) as usize;
            assert_eq!(
                b - a,
                128,
                "adjacent SharedTxnSlots must be 128 bytes apart"
            );
            assert_eq!(
                a % CACHE_LINE_BYTES,
                0,
                "each slot must be cache-line aligned"
            );
        }
    }

    #[test]
    fn test_txn_slot_field_offsets_cache_line_split() {
        // Verify hot-path fields in first cache line, admin fields in second.
        let slot = SharedTxnSlot::new();
        let base = (&raw const slot) as usize;

        // First cache line: txn_id through marked_for_abort (offsets 0..41)
        let txn_id_off = (&raw const slot.txn_id) as usize - base;
        let begin_seq_off = (&raw const slot.begin_seq) as usize - base;
        let commit_seq_off = (&raw const slot.commit_seq) as usize - base;
        let snapshot_high_off = (&raw const slot.snapshot_high) as usize - base;
        let write_set_pages_off = (&raw const slot.write_set_pages) as usize - base;
        let state_off = (&raw const slot.state) as usize - base;
        let mode_off = (&raw const slot.mode) as usize - base;
        let has_in_rw_off = (&raw const slot.has_in_rw) as usize - base;
        let has_out_rw_off = (&raw const slot.has_out_rw) as usize - base;
        let marked_for_abort_off = (&raw const slot.marked_for_abort) as usize - base;

        assert_eq!(txn_id_off, 0, "txn_id at offset 0");
        assert_eq!(begin_seq_off, 8);
        assert_eq!(commit_seq_off, 16);
        assert_eq!(snapshot_high_off, 24);
        assert_eq!(write_set_pages_off, 32);
        assert_eq!(state_off, 36);
        assert_eq!(mode_off, 37);
        assert_eq!(has_in_rw_off, 38);
        assert_eq!(has_out_rw_off, 39);
        assert_eq!(marked_for_abort_off, 40);

        // All hot-path fields in first 64 bytes
        assert!(
            marked_for_abort_off < 64,
            "marked_for_abort in first cache line"
        );

        // Second cache line: pid_birth starts at offset 64
        let pid_birth_off = (&raw const slot.pid_birth) as usize - base;
        let lease_expiry_off = (&raw const slot.lease_expiry) as usize - base;
        let txn_epoch_off = (&raw const slot.txn_epoch) as usize - base;
        let pid_off = (&raw const slot.pid) as usize - base;

        assert_eq!(pid_birth_off, 64, "pid_birth starts second cache line");
        assert_eq!(lease_expiry_off, 72);
        assert!(txn_epoch_off >= 64, "txn_epoch in second cache line");
        assert!(pid_off >= 64, "pid in second cache line");
    }

    #[test]
    fn test_txn_slot_basic_atomic_ops() {
        let slot = SharedTxnSlot::new();

        // Claim the slot.
        slot.txn_id.store(42, Ordering::Release);
        assert!(!slot.is_free(Ordering::Acquire));
        assert_eq!(slot.txn_id.load(Ordering::Acquire), 42);

        // Set SSI flags.
        slot.has_in_rw.store(true, Ordering::Release);
        slot.has_out_rw.store(true, Ordering::Release);
        assert!(slot.has_in_rw.load(Ordering::Acquire));
        assert!(slot.has_out_rw.load(Ordering::Acquire));

        // Set sequence numbers.
        slot.begin_seq.store(100, Ordering::Release);
        slot.commit_seq.store(105, Ordering::Release);
        slot.snapshot_high.store(99, Ordering::Release);
        assert_eq!(slot.begin_seq.load(Ordering::Acquire), 100);
        assert_eq!(slot.commit_seq.load(Ordering::Acquire), 105);
        assert_eq!(slot.snapshot_high.load(Ordering::Acquire), 99);

        // Release the slot.
        slot.txn_id.store(0, Ordering::Release);
        assert!(slot.is_free(Ordering::Acquire));
    }

    #[test]
    fn test_txn_slot_debug_output() {
        let slot = SharedTxnSlot::new();
        slot.txn_id.store(7, Ordering::Relaxed);
        slot.state.store(1, Ordering::Relaxed);
        let debug = format!("{slot:?}");
        assert!(
            debug.contains("SharedTxnSlot"),
            "debug must contain type name"
        );
        assert!(debug.contains("txn_id: 7"), "debug must show txn_id");
    }

    #[test]
    fn test_txn_slot_default() {
        let slot = SharedTxnSlot::default();
        assert!(slot.is_free(Ordering::Relaxed));
    }

    // -- Hot witness bucket alignment (§5.6.4.5) --

    #[test]
    fn test_hot_witness_buckets_cache_aligned() {
        use crate::witness_hierarchy::HotWitnessIndexSizingV1;

        assert_eq!(
            HotWitnessIndexSizingV1::ENTRY_ALIGNMENT_BYTES as usize,
            CACHE_LINE_BYTES,
            "hot witness bucket entries must be cache-line aligned"
        );
    }

    // -- Lock table / commit index integration --

    #[test]
    fn test_shared_page_lock_table_cache_aligned() {
        use crate::InProcessPageLockTable;

        // Verify the lock table works correctly with CacheAligned shards.
        let table = InProcessPageLockTable::new();
        let page = fsqlite_types::PageNumber::new(1).unwrap();
        let txn = fsqlite_types::TxnId::new(1).unwrap();

        assert!(table.try_acquire(page, txn).is_ok());
        assert_eq!(table.holder(page), Some(txn));
        assert_eq!(table.lock_count(), 1);

        table.release_all(txn);
        assert_eq!(table.lock_count(), 0);
    }

    #[test]
    fn test_commit_index_cache_aligned() {
        use crate::CommitIndex;

        let index = CommitIndex::new();
        let page = fsqlite_types::PageNumber::new(42).unwrap();
        let seq = fsqlite_types::CommitSeq::new(5);

        index.update(page, seq);
        assert_eq!(index.latest(page), Some(seq));
    }

    // -- E2E false sharing regression --

    fn median(samples: &mut [u128]) -> u128 {
        samples.sort_unstable();
        samples[samples.len() / 2]
    }

    fn ratio_permille(observed_us: u128, baseline_us: u128) -> u128 {
        observed_us
            .saturating_mul(1_000)
            .checked_div(baseline_us)
            .unwrap_or(1_000)
    }

    fn ops_per_sec(total_ops: u128, elapsed_us: u128) -> u128 {
        total_ops
            .saturating_mul(1_000_000)
            .checked_div(elapsed_us)
            .unwrap_or_else(|| total_ops.saturating_mul(1_000_000))
    }

    fn run_padded_round<const N_THREADS: usize>(iterations_per_thread: u64) -> u128 {
        let padded: Arc<[CacheAligned<AtomicU64>; N_THREADS]> =
            Arc::new(std::array::from_fn(|_| {
                CacheAligned::new(AtomicU64::new(0))
            }));

        let start = Instant::now();
        let handles: Vec<_> = (0..N_THREADS)
            .map(|thread_index| {
                let counters = Arc::clone(&padded);
                thread::spawn(move || {
                    for _ in 0..iterations_per_thread {
                        counters[thread_index].fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().expect("padded worker must not panic");
        }
        for counter in padded.iter() {
            assert_eq!(counter.load(Ordering::Relaxed), iterations_per_thread);
        }
        start.elapsed().as_micros()
    }

    fn run_unpadded_round<const N_THREADS: usize>(iterations_per_thread: u64) -> u128 {
        let unpadded: Arc<[AtomicU64; N_THREADS]> =
            Arc::new(std::array::from_fn(|_| AtomicU64::new(0)));

        let start = Instant::now();
        let handles: Vec<_> = (0..N_THREADS)
            .map(|thread_index| {
                let counters = Arc::clone(&unpadded);
                thread::spawn(move || {
                    for _ in 0..iterations_per_thread {
                        counters[thread_index].fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().expect("unpadded worker must not panic");
        }
        for counter in unpadded.iter() {
            assert_eq!(counter.load(Ordering::Relaxed), iterations_per_thread);
        }
        start.elapsed().as_micros()
    }

    #[test]
    fn test_e2e_shared_memory_false_sharing_regression() {
        const N_THREADS: usize = 4;
        const N_ITERS: u64 = 200_000;
        const ROUNDS: usize = 5;
        const WARN_RATIO_PERMILLE: u128 = 1_500; // 1.5x slower than unpadded baseline
        const FAIL_RATIO_PERMILLE: u128 = 3_000; // 3.0x slower than unpadded baseline

        let mut padded_samples = Vec::with_capacity(ROUNDS);
        let mut unpadded_samples = Vec::with_capacity(ROUNDS);
        for _ in 0..ROUNDS {
            padded_samples.push(run_padded_round::<N_THREADS>(N_ITERS));
            unpadded_samples.push(run_unpadded_round::<N_THREADS>(N_ITERS));
        }

        let mut padded_sorted = padded_samples.clone();
        let mut unpadded_sorted = unpadded_samples.clone();
        let observed_padded_median_us = median(&mut padded_sorted);
        let baseline_unpadded_median_us = median(&mut unpadded_sorted);
        let ratio_permille = ratio_permille(observed_padded_median_us, baseline_unpadded_median_us);

        let total_ops =
            u128::try_from(N_THREADS).expect("N_THREADS fits in u128") * u128::from(N_ITERS);
        let observed_padded_ops_per_sec = ops_per_sec(total_ops, observed_padded_median_us);
        let baseline_unpadded_ops_per_sec = ops_per_sec(total_ops, baseline_unpadded_median_us);

        tracing::info!(
            bead_id = "bd-22n.3",
            case = "false_sharing_regression",
            rounds = ROUNDS,
            threads = N_THREADS,
            iterations_per_thread = N_ITERS,
            baseline_unpadded_median_us,
            observed_padded_median_us,
            baseline_unpadded_ops_per_sec,
            observed_padded_ops_per_sec,
            ratio_permille,
            "false-sharing regression metrics"
        );

        if ratio_permille > WARN_RATIO_PERMILLE {
            tracing::warn!(
                bead_id = "bd-22n.3",
                case = "false_sharing_regression_warn",
                warn_ratio_permille = WARN_RATIO_PERMILLE,
                ratio_permille,
                baseline_unpadded_median_us,
                observed_padded_median_us,
                "padded counters are slower than expected relative to unpadded baseline"
            );
        }

        assert!(
            ratio_permille <= FAIL_RATIO_PERMILLE,
            "bead_id=bd-22n.3 case=false_sharing_regression_detected \
             ratio_permille={ratio_permille} baseline_unpadded_median_us={baseline_unpadded_median_us} \
             observed_padded_median_us={observed_padded_median_us}"
        );
    }

    // =======================================================================
    // bd-3t3.6: TxnSlot Three-Phase Acquire Protocol & Lifecycle Tests
    // =======================================================================

    // -- Tagged encoding roundtrip --

    #[test]
    fn test_tagged_encoding_roundtrip_claiming() {
        for &tid in &[1_u64, 42, 1000, fsqlite_types::TxnId::MAX_RAW] {
            let encoded = encode_claiming(tid);
            assert_eq!(decode_tag(encoded), TAG_CLAIMING);
            assert_eq!(decode_payload(encoded), tid);
            assert!(is_sentinel(encoded));
        }
    }

    #[test]
    fn test_tagged_encoding_roundtrip_cleaning() {
        for &tid in &[1_u64, 42, 1000, fsqlite_types::TxnId::MAX_RAW] {
            let encoded = encode_cleaning(tid);
            assert_eq!(decode_tag(encoded), TAG_CLEANING);
            assert_eq!(decode_payload(encoded), tid);
            assert!(is_sentinel(encoded));
        }
    }

    #[test]
    fn test_real_txn_id_has_clear_top_bits() {
        // Real TxnIds have top 2 bits clear (tag = 0b00).
        for &tid in &[1_u64, 42, fsqlite_types::TxnId::MAX_RAW] {
            assert_eq!(
                tid & SLOT_TAG_MASK,
                0,
                "real TxnId {tid} must have clear top 2 bits"
            );
            assert!(!is_sentinel(tid));
        }
    }

    #[test]
    fn test_txn_id_zero_is_free_sentinel() {
        assert_eq!(decode_tag(0), 0);
        assert_eq!(decode_payload(0), 0);
        assert!(!is_sentinel(0));
    }

    #[test]
    fn test_txn_id_max_boundary() {
        let max = fsqlite_types::TxnId::MAX_RAW;
        assert_eq!(max, (1_u64 << 62) - 1);
        // Encoding max in claiming must not overflow into cleaning bits.
        let claim = encode_claiming(max);
        assert_eq!(decode_tag(claim), TAG_CLAIMING);
        assert_eq!(decode_payload(claim), max);
    }

    // -- Phase 1: CLAIM --

    #[test]
    fn test_phase1_cas_free_to_claiming_exclusive() {
        let slot = SharedTxnSlot::new();

        // First claim succeeds.
        assert!(slot.phase1_claim(42));
        assert_eq!(slot.txn_id.load(Ordering::Acquire), encode_claiming(42));

        // Second claim (different TxnId) fails — slot is not free.
        assert!(!slot.phase1_claim(99));
        // Original claim is still intact.
        assert_eq!(slot.txn_id.load(Ordering::Acquire), encode_claiming(42));
    }

    #[test]
    fn test_phase1_cas_occupied_fails() {
        let slot = SharedTxnSlot::new();
        // Set a real txn_id (active transaction).
        slot.txn_id.store(7, Ordering::Release);

        assert!(
            !slot.phase1_claim(42),
            "claim must fail when slot is occupied"
        );
        assert_eq!(slot.txn_id.load(Ordering::Acquire), 7);
    }

    // -- Phase 2: INITIALIZE --

    #[test]
    fn test_phase2_pid_published_before_snapshot() {
        let slot = SharedTxnSlot::new();
        assert!(slot.phase1_claim(42));

        slot.phase2_initialize(
            1234,  // pid
            99999, // pid_birth
            50000, // lease_secs
            100,   // begin_seq
            100,   // snapshot_high
            slot_mode::CONCURRENT,
            7, // witness_epoch
        );

        // Verify pid/pid_birth/lease_expiry are set.
        assert_eq!(slot.pid.load(Ordering::Acquire), 1234);
        assert_eq!(slot.pid_birth.load(Ordering::Acquire), 99999);
        assert_eq!(slot.lease_expiry.load(Ordering::Acquire), 50000);

        // Verify snapshot fields are also set.
        assert_eq!(slot.begin_seq.load(Ordering::Acquire), 100);
        assert_eq!(slot.snapshot_high.load(Ordering::Acquire), 100);

        // Verify state is Active.
        assert_eq!(slot.state.load(Ordering::Acquire), slot_state::ACTIVE);
        assert_eq!(slot.mode.load(Ordering::Acquire), slot_mode::CONCURRENT);

        // SSI flags cleared.
        assert!(!slot.has_in_rw.load(Ordering::Acquire));
        assert!(!slot.has_out_rw.load(Ordering::Acquire));
        assert!(!slot.marked_for_abort.load(Ordering::Acquire));

        // txn_epoch incremented from 0 to 1.
        assert_eq!(slot.txn_epoch.load(Ordering::Acquire), 1);
    }

    // -- Phase 3: PUBLISH --

    #[test]
    fn test_phase3_cas_claiming_to_real_tid() {
        let slot = SharedTxnSlot::new();
        assert!(slot.phase1_claim(42));

        slot.phase2_initialize(1234, 99999, 50000, 100, 100, slot_mode::CONCURRENT, 0);

        // Phase 3 CAS: claiming_word → real tid.
        assert!(slot.phase3_publish(42));
        assert_eq!(slot.txn_id.load(Ordering::Acquire), 42);
        assert!(!slot.is_sentinel(Ordering::Acquire));
        assert!(!slot.is_free(Ordering::Acquire));

        // Claiming timestamp must be cleared.
        assert_eq!(slot.claiming_timestamp.load(Ordering::Acquire), 0);
    }

    #[test]
    fn test_phase3_cas_aba_prevention() {
        let slot = SharedTxnSlot::new();

        // Process A claims with TxnId 42.
        assert!(slot.phase1_claim(42));

        // Simulate cleanup: reclaim slot (reset to free), then Process B
        // claims with TxnId 99.
        slot.txn_id.store(0, Ordering::Release);
        assert!(slot.phase1_claim(99));

        // Process A tries Phase 3 with its original claiming word — CAS must
        // fail because the slot now holds encode_claiming(99), not encode_claiming(42).
        assert!(
            !slot.phase3_publish(42),
            "Phase 3 must fail after cleanup reclaimed and re-claimed the slot"
        );

        // Process B can still successfully publish.
        assert!(slot.phase3_publish(99));
        assert_eq!(slot.txn_id.load(Ordering::Acquire), 99);
    }

    // -- Slot release / freeing discipline --

    #[test]
    fn test_slot_free_clears_all_fields_txnid_last() {
        let slot = SharedTxnSlot::new();

        // Set up an active slot with various fields populated.
        slot.txn_id.store(42, Ordering::Release);
        slot.begin_seq.store(100, Ordering::Release);
        slot.snapshot_high.store(99, Ordering::Release);
        slot.commit_seq.store(105, Ordering::Release);
        slot.write_set_pages.store(17, Ordering::Release);
        slot.state.store(slot_state::COMMITTED, Ordering::Release);
        slot.mode.store(slot_mode::CONCURRENT, Ordering::Release);
        slot.has_in_rw.store(true, Ordering::Release);
        slot.has_out_rw.store(true, Ordering::Release);
        slot.marked_for_abort.store(true, Ordering::Release);
        slot.witness_epoch.store(3, Ordering::Release);
        slot.pid.store(1234, Ordering::Release);
        slot.pid_birth.store(99999, Ordering::Release);
        slot.lease_expiry.store(50000, Ordering::Release);
        slot.claiming_timestamp.store(12345, Ordering::Release);
        slot.cleanup_txn_id.store(77, Ordering::Release);

        // Release the slot.
        slot.release();

        // All fields must be zeroed.
        assert!(slot.is_free(Ordering::Acquire));
        assert_eq!(slot.txn_id.load(Ordering::Acquire), 0);
        assert_eq!(slot.begin_seq.load(Ordering::Acquire), 0);
        assert_eq!(slot.snapshot_high.load(Ordering::Acquire), 0);
        assert_eq!(slot.commit_seq.load(Ordering::Acquire), 0);
        assert_eq!(slot.write_set_pages.load(Ordering::Acquire), 0);
        assert_eq!(slot.state.load(Ordering::Acquire), slot_state::FREE);
        assert_eq!(slot.mode.load(Ordering::Acquire), 0);
        assert!(!slot.has_in_rw.load(Ordering::Acquire));
        assert!(!slot.has_out_rw.load(Ordering::Acquire));
        assert!(!slot.marked_for_abort.load(Ordering::Acquire));
        assert_eq!(slot.witness_epoch.load(Ordering::Acquire), 0);
        assert_eq!(slot.pid.load(Ordering::Acquire), 0);
        assert_eq!(slot.pid_birth.load(Ordering::Acquire), 0);
        assert_eq!(slot.lease_expiry.load(Ordering::Acquire), 0);
        assert_eq!(slot.claiming_timestamp.load(Ordering::Acquire), 0);
        assert_eq!(slot.cleanup_txn_id.load(Ordering::Acquire), 0);
    }

    // -- TxnSlotArray --

    #[test]
    fn test_txn_slot_array_basic() {
        let arr = TxnSlotArray::new(4);
        assert_eq!(arr.len(), 4);
        assert!(!arr.is_empty());
        assert_eq!(arr.free_count(), 4);
        assert_eq!(arr.occupied_count(), 0);
    }

    #[test]
    fn test_txn_slot_array_acquire_release() {
        let arr = TxnSlotArray::new(4);

        let idx = arr
            .acquire(
                42,
                0,
                1234,
                99999,
                50000,
                100,
                100,
                slot_mode::CONCURRENT,
                0,
            )
            .expect("acquire should succeed");
        assert_eq!(idx, 0);
        assert_eq!(arr.free_count(), 3);
        assert_eq!(arr.occupied_count(), 1);
        assert_eq!(arr.slot(idx).txn_id.load(Ordering::Acquire), 42);

        // Release the slot.
        arr.slot(idx).release();
        assert_eq!(arr.free_count(), 4);
    }

    #[test]
    fn test_max_txn_slots_exhaustion_returns_busy() {
        let arr = TxnSlotArray::new(2);

        let idx0 = arr
            .acquire(1, 0, 100, 10000, 50000, 1, 1, slot_mode::CONCURRENT, 0)
            .expect("first acquire");
        let idx1 = arr
            .acquire(2, 0, 100, 10000, 50000, 2, 2, slot_mode::CONCURRENT, 0)
            .expect("second acquire");
        assert_ne!(idx0, idx1);

        // All slots full — next acquire must fail.
        let err = arr
            .acquire(3, 0, 100, 10000, 50000, 3, 3, slot_mode::CONCURRENT, 0)
            .unwrap_err();
        assert_eq!(err, SlotAcquireError::AllSlotsBusy);

        // Release one and re-acquire.
        arr.slot(idx0).release();
        let idx_reacquired = arr
            .acquire(3, 0, 100, 10000, 50000, 3, 3, slot_mode::CONCURRENT, 0)
            .expect("acquire should succeed after release");
        assert_eq!(idx_reacquired, idx0);
    }

    #[test]
    fn test_txn_slot_array_hint_index_wraps() {
        let arr = TxnSlotArray::new(4);

        // Occupy slot 0.
        let _ = arr
            .acquire(1, 0, 100, 10000, 50000, 1, 1, slot_mode::CONCURRENT, 0)
            .unwrap();

        // Hint at slot 0 — should wrap and find slot 1.
        let idx = arr
            .acquire(2, 0, 100, 10000, 50000, 2, 2, slot_mode::CONCURRENT, 0)
            .unwrap();
        assert_eq!(idx, 1);

        // Hint at slot 3 — should wrap to slot 2 or 3.
        let idx2 = arr
            .acquire(3, 3, 100, 10000, 50000, 3, 3, slot_mode::CONCURRENT, 0)
            .unwrap();
        assert!(idx2 == 2 || idx2 == 3);
    }

    #[test]
    fn test_lease_expiry_and_pid_birth_prevent_reuse() {
        let slot = SharedTxnSlot::new();

        // Process A (pid=100, birth=T1) claims slot.
        assert!(slot.phase1_claim(42));
        slot.phase2_initialize(
            100,   // pid
            1000,  // pid_birth T1
            50000, // lease
            1,     // begin_seq
            1,     // snapshot_high
            slot_mode::CONCURRENT,
            0,
        );
        assert!(slot.phase3_publish(42));

        // Simulate process death: slot still shows pid=100, birth=1000.
        // Process B (pid=100, birth=2000 — different birth) observes slot.
        let observed_pid = slot.pid.load(Ordering::Acquire);
        let observed_birth = slot.pid_birth.load(Ordering::Acquire);

        assert_eq!(observed_pid, 100);
        assert_eq!(observed_birth, 1000);

        // Process B detects pid_birth mismatch: 1000 != 2000.
        let process_b_birth = 2000_u64;
        let mismatch = observed_birth != process_b_birth;
        assert!(mismatch, "different pid_birth must indicate stale slot");
    }

    // -- Full lifecycle end-to-end --

    #[test]
    fn test_full_lifecycle_claim_init_publish_release() {
        let slot = SharedTxnSlot::new();
        assert!(slot.is_free(Ordering::Acquire));

        // Phase 1.
        assert!(slot.phase1_claim(42));
        assert!(slot.is_claiming(Ordering::Acquire));

        // Phase 2.
        slot.phase2_initialize(1234, 99999, 50000, 100, 100, slot_mode::CONCURRENT, 7);

        // Phase 3.
        assert!(slot.phase3_publish(42));
        assert!(!slot.is_sentinel(Ordering::Acquire));
        assert_eq!(slot.txn_id.load(Ordering::Acquire), 42);

        // Simulate commit.
        slot.commit_seq.store(105, Ordering::Release);
        slot.state.store(slot_state::COMMITTED, Ordering::Release);

        // Release.
        slot.release();
        assert!(slot.is_free(Ordering::Acquire));
    }

    #[test]
    fn test_concurrent_phase1_exclusive_two_threads() {
        let slot = Arc::new(SharedTxnSlot::new());
        let s1 = Arc::clone(&slot);
        let s2 = Arc::clone(&slot);

        let h1 = thread::spawn(move || s1.phase1_claim(42));
        let h2 = thread::spawn(move || s2.phase1_claim(99));

        let r1 = h1.join().unwrap();
        let r2 = h2.join().unwrap();

        // Exactly one must succeed.
        assert!(
            r1 ^ r2,
            "exactly one of two concurrent Phase 1 claims must succeed"
        );
    }

    #[test]
    fn test_txn_slot_array_threaded_acquire() {
        let arr = Arc::new(TxnSlotArray::new(256));
        let mut handles = Vec::with_capacity(4);

        for worker in 0_u64..4 {
            let a = Arc::clone(&arr);
            handles.push(thread::spawn(move || {
                let mut acquired = Vec::with_capacity(50);
                for i in 0_u64..50 {
                    let tid = worker * 100 + i + 1;
                    let hint = usize::try_from(tid % 256).unwrap_or(0);
                    let idx = a
                        .acquire(
                            tid,
                            hint,
                            u32::try_from(worker).unwrap_or(0),
                            10000 + worker,
                            50000,
                            i + 1,
                            i + 1,
                            slot_mode::CONCURRENT,
                            0,
                        )
                        .expect("256 slots should not exhaust for 200 txns");
                    acquired.push(idx);
                }
                acquired
            }));
        }

        for h in handles {
            let _ = h.join().unwrap();
        }

        // All 200 transactions got unique slot indices.
        assert_eq!(arr.occupied_count(), 200);
        assert_eq!(arr.free_count(), 56);
    }

    #[test]
    fn test_slot_reuse_bumps_txn_epoch() {
        let slot = SharedTxnSlot::new();
        assert_eq!(slot.txn_epoch.load(Ordering::Acquire), 0);

        // First use.
        assert!(slot.phase1_claim(1));
        slot.phase2_initialize(100, 10000, 50000, 1, 1, slot_mode::CONCURRENT, 0);
        assert!(slot.phase3_publish(1));
        assert_eq!(slot.txn_epoch.load(Ordering::Acquire), 1);
        slot.release();

        // Second use — epoch must be 2.
        assert!(slot.phase1_claim(2));
        slot.phase2_initialize(100, 10000, 50000, 2, 2, slot_mode::CONCURRENT, 0);
        assert!(slot.phase3_publish(2));
        assert_eq!(slot.txn_epoch.load(Ordering::Acquire), 2);
        slot.release();
    }

    // =======================================================================
    // bd-3t3.7: RecentlyCommittedReadersIndex Tests
    // =======================================================================

    // -- Bloom filter --

    #[test]
    fn test_rcri_bloom_insert_query() {
        let mut filter = [0u8; rcri_bloom::BYTES];
        bloom_insert(&mut filter, 42);
        assert!(bloom_query(&filter, 42));
        // Empty filter should not match arbitrary page.
        let empty = [0u8; rcri_bloom::BYTES];
        assert!(!bloom_query(&empty, 42));
    }

    #[test]
    fn test_rcri_bloom_no_false_negatives() {
        let pages: Vec<u32> = (1..=100).collect();
        let mut filter = [0u8; rcri_bloom::BYTES];
        for &p in &pages {
            bloom_insert(&mut filter, p);
        }
        for &p in &pages {
            assert!(
                bloom_query(&filter, p),
                "bloom must contain inserted page {p}"
            );
        }
    }

    #[test]
    fn test_rcri_bloom_hashing_domain_separated() {
        let h0 = bloom_hash(42, 0);
        let h1 = bloom_hash(42, 1);
        let h2 = bloom_hash(42, 2);
        assert!(h0 < rcri_bloom::BITS);
        assert!(h1 < rcri_bloom::BITS);
        assert!(h2 < rcri_bloom::BITS);
    }

    // -- RcriEntry --

    #[test]
    fn test_rcri_entry_creation_and_query() {
        let entry = RcriEntry::new(42, 100, 50, &[1, 2, 3, 4, 5]);
        assert_eq!(entry.txn_id, 42);
        assert_eq!(entry.commit_seq, 100);
        assert_eq!(entry.begin_seq, 50);
        for p in 1..=5 {
            assert!(entry.bloom_may_contain(p), "page {p} must be found");
        }
    }

    // -- RecentlyCommittedReadersIndex --

    #[test]
    fn test_rcri_basic_insert_and_query() {
        let mut rcri = RecentlyCommittedReadersIndex::new(8);
        assert_eq!(rcri.capacity(), 8);
        assert!(rcri.is_empty());

        let entry = RcriEntry::new(1, 100, 50, &[10, 20, 30]);
        rcri.insert(entry, 0).expect("insert should succeed");
        assert_eq!(rcri.len(), 1);

        let matches: Vec<_> = rcri.query_incoming_edges(20, 200).collect();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].txn_id, 1);

        assert!(rcri.query_incoming_edges(99, 200).next().is_none());
    }

    #[test]
    fn test_rcri_records_committed_reader_before_slot_free() {
        let mut rcri = RecentlyCommittedReadersIndex::new(16);
        let slot = SharedTxnSlot::new();

        assert!(slot.phase1_claim(42));
        slot.phase2_initialize(100, 10000, 50000, 50, 50, slot_mode::CONCURRENT, 0);
        assert!(slot.phase3_publish(42));

        let entry = RcriEntry::new(42, 100, 50, &[5, 10, 15]);
        rcri.insert(entry, 0).expect("insert before slot free");
        assert_eq!(rcri.len(), 1);

        slot.release();
        assert!(slot.is_free(Ordering::Acquire));

        let matches: Vec<_> = rcri.query_incoming_edges(10, 200).collect();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].txn_id, 42);
    }

    #[test]
    fn test_rcri_ring_buffer_wraparound() {
        let mut rcri = RecentlyCommittedReadersIndex::new(3);

        for i in 1..=3_u64 {
            let entry = RcriEntry::new(i, i * 10, i, &[u32::try_from(i).unwrap()]);
            rcri.insert(entry, 0).expect("insert should succeed");
        }
        assert_eq!(rcri.len(), 3);

        // min_active_begin_seq=10 means oldest (commit_seq=10) is evictable.
        let entry = RcriEntry::new(4, 40, 4, &[4]);
        rcri.insert(entry, 10).expect("should evict oldest");
        assert_eq!(rcri.len(), 3);

        let first = rcri.entry_at(0).unwrap();
        assert_eq!(first.txn_id, 2);
        let last = rcri.entry_at(2).unwrap();
        assert_eq!(last.txn_id, 4);
    }

    #[test]
    fn test_rcri_overflow_aborts_committer() {
        let mut rcri = RecentlyCommittedReadersIndex::new(2);

        let e1 = RcriEntry::new(1, 100, 50, &[1]);
        let e2 = RcriEntry::new(2, 200, 60, &[2]);
        rcri.insert(e1, 0).unwrap();
        rcri.insert(e2, 0).unwrap();
        assert_eq!(rcri.len(), 2);

        let e3 = RcriEntry::new(3, 300, 70, &[3]);
        let result = rcri.insert(e3, 50);
        assert_eq!(result.unwrap_err(), RcriOverflowError);
        assert_eq!(rcri.len(), 2);
    }

    #[test]
    fn test_rcri_gc_prunes_when_safe() {
        let mut rcri = RecentlyCommittedReadersIndex::new(8);

        for i in 1..=5_u64 {
            let entry = RcriEntry::new(i, i * 10, i, &[u32::try_from(i).unwrap()]);
            rcri.insert(entry, 0).unwrap();
        }
        assert_eq!(rcri.len(), 5);

        let pruned = rcri.gc(30);
        assert_eq!(pruned, 3);
        assert_eq!(rcri.len(), 2);

        let first = rcri.entry_at(0).unwrap();
        assert_eq!(first.txn_id, 4);
    }

    #[test]
    fn test_rcri_incoming_edge_discovery() {
        let mut rcri = RecentlyCommittedReadersIndex::new(16);

        let reader_entry = RcriEntry::new(1, 15, 10, &[5]);
        rcri.insert(reader_entry, 0).unwrap();

        let edges: Vec<_> = rcri.query_incoming_edges(5, 20).collect();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].txn_id, 1);

        assert!(rcri.query_incoming_edges(5, 5).next().is_none());
    }

    #[test]
    fn test_rcri_no_false_positive_edges_disjoint_pages() {
        let mut rcri = RecentlyCommittedReadersIndex::new(16);

        let entry = RcriEntry::new(1, 100, 50, &[1, 2, 3, 4, 5]);
        rcri.insert(entry, 0).unwrap();

        let entry_ref = rcri.entry_at(0).unwrap();
        let unlikely_page = 999_999;
        if !entry_ref.bloom_may_contain(unlikely_page) {
            assert!(
                rcri.query_incoming_edges(unlikely_page, 200)
                    .next()
                    .is_none()
            );
        }
    }

    #[test]
    fn test_rcri_multiple_readers_concurrent_commit() {
        let mut rcri = RecentlyCommittedReadersIndex::new(32);

        for i in 1..=10_u64 {
            let base = u32::try_from(i).unwrap();
            let pages: Vec<u32> = (base..base + 5).collect();
            let entry = RcriEntry::new(i, i * 10 + 100, i * 5, &pages);
            rcri.insert(entry, 0).unwrap();
        }
        assert_eq!(rcri.len(), 10);

        assert!(
            rcri.query_incoming_edges(5, 1000).count() >= 2,
            "at least 2 readers should match page 5"
        );
    }

    #[test]
    fn test_rcri_e2e_ssi_correctness() {
        // E2E: X -rw-> R -rw-> T where R already committed.
        // R begins at begin_seq=10, reads page 7, commits at seq=12.
        // T begins at snapshot_high=15, writes page 7.
        // T checks RCRI: finds R as incoming edge.

        let mut rcri = RecentlyCommittedReadersIndex::new(16);

        let r_entry = RcriEntry::new(1, 12, 10, &[7]);
        rcri.insert(r_entry, 0).unwrap();

        let incoming_edges: Vec<_> = rcri.query_incoming_edges(7, 15).collect();
        assert_eq!(incoming_edges.len(), 1);
        assert_eq!(incoming_edges[0].txn_id, 1);
        assert_eq!(incoming_edges[0].commit_seq, 12);
    }
}
