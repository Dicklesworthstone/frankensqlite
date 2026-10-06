//! Concrete snapshot page transactions over the native durable driver.
//!
//! Open transactions own private overlays; they do not hold a store borrow or
//! a file lock. Only publication requires exclusive access to this owner. This
//! is an in-process page API, not the sealed SQL pager or cross-process MVCC.
//! The caller retains the native log's namespace/append lease, including while
//! an abandoned tracked write settles. The store never creates or deletes files.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::{CommitSeq, ObjectId, PageNumber, SymbolRecord, TxnEpoch, TxnId, TxnToken};
use fsqlite_vfs::{VfsFile, VfsWriteCompletion};

use super::{MAX_CAPSULE_PAGES, MAX_PAGE_CAPSULE_BYTES, NativePageCapsule, NativePageWrite, corrupt, validate_page_size};
use crate::native_commit::{CommitResult, CommitSubmission};
use crate::native_commit::durable::{DurableCommitAcknowledgement, DurableCommitError, DurableWriteCoordinator, NativeObjectCodec};
use crate::native_durability::{NativeDurabilityLimits, NativeDurabilityLog, NativeDurabilityRecovery};

/// Retained page payload and metadata bounds, not a process-RSS claim.
/// Obsolete page versions can be reclaimed, but every open snapshot keeps its
/// floor version and all newer versions. Admission attempts reclamation before
/// refusing an over-budget write; pinned history is never evicted. This bounds
/// retained page images, not caller-held Arcs, object indexes, or durable logs.
#[derive(Debug, Clone, Copy)]
pub struct NativePageLimits {
    pub max_retained_page_bytes: usize,
    pub max_versions: usize,
    pub max_active_transactions: usize,
}
impl Default for NativePageLimits {
    fn default() -> Self {
        Self {
            max_retained_page_bytes: 64 * 1024 * 1024,
            max_versions: 131_072,
            max_active_transactions: 128,
        }
    }
}
impl NativePageLimits {
    fn validate(self) -> Result<()> {
        if self.max_retained_page_bytes == 0 || self.max_versions == 0
            || self.max_active_transactions == 0 || self.max_active_transactions > 65_536
        {
            return Err(FrankenError::TooBig);
        }
        Ok(())
    }
}

/// An indeterminate transaction cannot be rolled back or retried as though
/// its marker were absent. Reopen/recovery must resolve the stored history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativePageTransactionState {
    Active,
    Committed,
    RolledBack,
    Indeterminate,
}

/// Opaque marker for a transaction's private overlay. It is neither a commit
/// receipt nor a database-wide snapshot. Released or foreign markers fail closed.
#[derive(Clone)]
pub struct NativePageSavepoint {
    owner: Arc<()>,
    token: TxnToken,
    id: u64,
}

struct SavedOverlay {
    id: u64,
    writes: BTreeMap<PageNumber, Option<Arc<[u8]>>>,
    payload_bytes: usize,
}

/// Nested overlay checkpoints are bounded independently of the active overlay.
pub const MAX_NATIVE_SAVEPOINTS: usize = 32;

/// An owner-bound snapshot and private overlay. Dropping an active transaction
/// discards only this private overlay and releases its active-session slot.
/// Already returned page Arcs may outlive it and are caller-owned memory.
pub struct NativePageTransaction {
    owner: Arc<()>,
    // Pin from BEGIN, not from the first page read: lazy readers need their
    // entire snapshot even when they have not observed a single page yet.
    lease: Option<Arc<CommitSeq>>,
    token: TxnToken,
    snapshot: CommitSeq,
    // Bounds describe this snapshot, never another writer's reservations.
    snapshot_extent: u32,
    issued_extent: u32,
    completed_extent: u32,
    reservations: BTreeSet<PageNumber>,
    reads: BTreeMap<PageNumber, CommitSeq>,
    writes: BTreeMap<PageNumber, Option<Arc<[u8]>>>,
    payload_bytes: usize,
    state: NativePageTransactionState,
    savepoints: Vec<SavedOverlay>,
    next_savepoint_id: u64,
}
impl NativePageTransaction {
    #[must_use]
    pub const fn snapshot(&self) -> CommitSeq { self.snapshot }
    #[must_use]
    pub const fn token(&self) -> TxnToken { self.token }
    #[must_use]
    pub const fn state(&self) -> NativePageTransactionState { self.state }

    /// Highest committed page number at BEGIN, including tombstones. This is
    /// a logical address bound, not a count of present pages or a file length.
    /// It is unchanged by peer commits, private writes, and history reclamation.
    #[must_use]
    pub const fn snapshot_db_size(&self) -> u32 { self.snapshot_extent }

    /// Snapshot extent plus the current private commit surface. ROLLBACK TO
    /// removes discarded growth from this bound, but not from the allocator.
    /// A committed handle retains its own terminal extent after overlay cleanup.
    #[must_use]
    pub fn live_db_size(&self) -> u32 {
        if self.state == NativePageTransactionState::Committed {
            self.completed_extent
        } else {
            self.writes.last_key_value()
                .map_or(self.snapshot_extent, |(page, _)| self.snapshot_extent.max(page.get()))
        }
    }

    /// Largest address in the snapshot or successfully issued to this handle.
    /// Unlike `live_db_size`, this includes private addresses spent by rolled
    /// back operations. Neither bound authorizes reading an absent page.
    #[must_use]
    pub const fn visible_db_size_bound(&self) -> u32 { self.issued_extent }

    /// Newly introduced page addresses owned by this active transaction,
    /// including addresses spent before ROLLBACK TO. They are not a reusable
    /// SQLite freelist, and holes are never expanded into an unbounded vector.
    #[must_use]
    pub fn live_reserved_pages(&self) -> Vec<PageNumber> {
        self.reservations.iter().copied().collect()
    }

    /// Whether this active transaction has a private replacement or tombstone.
    #[must_use]
    pub fn is_page_dirty(&self, page: PageNumber) -> bool {
        self.state == NativePageTransactionState::Active && self.writes.contains_key(&page)
    }

    fn finish(&mut self, state: NativePageTransactionState) {
        if state == NativePageTransactionState::Committed {
            self.completed_extent = self.live_db_size();
        }
        self.state = state;
        self.lease = None;
        self.reads.clear();
        self.writes.clear();
        self.payload_bytes = 0;
        self.savepoints.clear();
        self.reservations.clear();
    }
}

struct SharedCodec<C>(Arc<C>);
impl<C: NativeObjectCodec> NativeObjectCodec for SharedCodec<C> {
    fn encode(&self, cx: &Cx, bytes: &[u8]) -> Result<Vec<SymbolRecord>> {
        self.0.encode(cx, bytes)
    }
    fn decode(&self, cx: &Cx, id: ObjectId, records: &[SymbolRecord]) -> Result<Vec<u8>> {
        self.0.decode(cx, id, records)
    }
}

struct Version {
    seq: CommitSeq,
    data: Option<Arc<[u8]>>,
}
struct PageHistory {
    page_size: u32,
    tip: CommitSeq,
    pages: BTreeMap<PageNumber, Vec<Version>>,
    tokens: HashSet<TxnToken>,
    max_txn_id: u64,
    payload_bytes: usize,
    version_count: usize,
    limits: NativePageLimits,
    reclaim_after: Option<PageNumber>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ApplyMode {
    /// Preserve the current head until durability succeeds, and older pins.
    Live,
    /// No reader can observe the local recovery builder: retain latest only.
    Replay,
}

struct PreparedApply {
    seq: CommitSeq,
    token: TxnToken,
    versions: Vec<(PageNumber, Version)>,
    payload_bytes: usize,
    version_count: usize,
    mode: ApplyMode,
}
impl PageHistory {
    fn new(page_size: u32, limits: NativePageLimits) -> Result<Self> {
        validate_page_size(page_size)?;
        limits.validate()?;
        Ok(Self {
            page_size, limits, tip: CommitSeq::ZERO, pages: BTreeMap::new(),
            tokens: HashSet::new(), max_txn_id: 0, payload_bytes: 0, version_count: 0,
            reclaim_after: None,
        })
    }
    fn at(&self, page: PageNumber, snapshot: CommitSeq) -> Option<&Version> {
        let versions = self.pages.get(&page)?;
        let count = versions.partition_point(|version| version.seq <= snapshot);
        count.checked_sub(1).and_then(|index| versions.get(index))
    }
    fn latest(&self, page: PageNumber) -> CommitSeq {
        self.pages.get(&page).and_then(|versions| versions.last())
            .map_or(CommitSeq::ZERO, |version| version.seq)
    }
    fn validate(&self, capsule: &NativePageCapsule) -> Result<()> {
        if capsule.page_size != self.page_size {
            return Err(corrupt("native page store/capsule page-size mismatch"));
        }
        capsule.validate_snapshot(self.tip, |page| self.latest(page))
    }
    fn projected_size(&self, capsule: &NativePageCapsule, mode: ApplyMode) -> Result<(usize, usize)> {
        let mut retained_versions = self.version_count;
        let mut retained_bytes = self.payload_bytes;
        if mode == ApplyMode::Replay {
            // Subtract ALL replaced heads before adding ANY new images. A
            // deletion may follow an insertion in canonical page order; its
            // released payload still belongs in the same atomic budget check.
            // Callers validate unique writes before reaching this function.
            for write in &capsule.writes {
                if let Some(versions) = self.pages.get(&write.page) {
                    retained_versions = retained_versions.checked_sub(versions.len())
                        .ok_or_else(|| corrupt("native replay version accounting underflow"))?;
                    for version in versions {
                        retained_bytes = retained_bytes
                            .checked_sub(version.data.as_ref().map_or(0, |data| data.len()))
                            .ok_or_else(|| corrupt("native replay payload accounting underflow"))?;
                    }
                }
            }
        }
        let version_count = retained_versions.checked_add(capsule.writes.len())
            .filter(|count| *count <= self.limits.max_versions).ok_or(FrankenError::TooBig)?;
        let payload_bytes = capsule.writes.iter().try_fold(retained_bytes, |total, write| {
            total.checked_add(write.data.as_ref().map_or(0, |data| data.len()))
                .filter(|n| *n <= self.limits.max_retained_page_bytes).ok_or(FrankenError::TooBig)
        })?;
        Ok((version_count, payload_bytes))
    }

    fn prepare(&mut self, capsule: &NativePageCapsule, seq: CommitSeq, token: TxnToken, mode: ApplyMode) -> Result<PreparedApply> {
        self.validate(capsule)?;
        if self.tip.get().checked_add(1) != Some(seq.get()) || self.tokens.contains(&token) {
            return Err(corrupt("noncontiguous or duplicate native page commit"));
        }
        let (version_count, payload_bytes) = self.projected_size(capsule, mode)?;
        let mut versions = Vec::new();
        versions.try_reserve_exact(capsule.writes.len()).map_err(|_| FrankenError::OutOfMemory)?;
        self.tokens.try_reserve(1).map_err(|_| FrankenError::OutOfMemory)?;
        for write in &capsule.writes {
            // Reserve all publication storage before any irreversible write.
            // Empty precreated entries have no visible page version.
            let chain = self.pages.entry(write.page).or_default();
            let extra = usize::from(mode == ApplyMode::Live || chain.is_empty());
            if chain.try_reserve(extra).is_err() {
                self.pages.retain(|_, versions| !versions.is_empty());
                return Err(FrankenError::OutOfMemory);
            }
            versions.push((write.page, Version { seq, data: write.data.clone() }));
        }
        Ok(PreparedApply { seq, token, versions, payload_bytes, version_count, mode })
    }
    fn apply(&mut self, prepared: PreparedApply) {
        for (page, version) in prepared.versions {
            let chain = self.pages.get_mut(&page).expect("page publication storage was reserved");
            if prepared.mode == ApplyMode::Replay {
                chain.clear();
            }
            chain.push(version);
        }
        self.tokens.insert(prepared.token);
        self.max_txn_id = self.max_txn_id.max(prepared.token.id.get());
        self.tip = prepared.seq;
        self.payload_bytes = prepared.payload_bytes;
        self.version_count = prepared.version_count;
    }

    fn reclaim(&mut self, cx: &Cx, floor: CommitSeq, page_budget: usize) -> Result<usize> {
        let last_page = self.pages.last_key_value().map(|(page, _)| *page);
        let start = self.reclaim_after.map_or(Unbounded, Excluded);
        let mut removed = 0;
        for (page, versions) in self.pages.range_mut((start, Unbounded)).take(page_budget) {
            cx.checkpoint().map_err(|_| FrankenError::Interrupt)?;
            // Keep the newest version <= floor, not merely versions >= floor.
            // A page unchanged for many commits may have a much older floor.
            // With no version <= floor, preserve all versions (old absence).
            let prune = versions.partition_point(|version| version.seq <= floor)
                .saturating_sub(1);
            if prune != 0 {
                let bytes: usize = versions[..prune].iter()
                    .map(|version| version.data.as_ref().map_or(0, |data| data.len()))
                    .sum();
                drop(versions.drain(..prune));
                versions.shrink_to_fit();
                self.payload_bytes -= bytes;
                self.version_count -= prune;
                removed += prune;
            }
            // Update progress and counters per page, so cancellation between
            // pages leaves a coherent history and a resumable sweep cursor.
            self.reclaim_after = Some(*page);
        }
        if self.reclaim_after >= last_page {
            self.reclaim_after = None;
        }
        // In particular, retain the latest tombstone and the exact token set.
        // Neither absence/conflict stamps nor replay identity are GC garbage.
        Ok(removed)
    }
}

/// Native full-page snapshot transactions, backed by the existing durable
/// coordinator, object codec, and two-stream recovery path. Its callbacks are
/// fixed by this implementation: callers cannot omit page conflict validation.
///
/// Page writes are buffered independently in transaction handles. Commit-order
/// read validation is conservative (not SSI). Public SQL dispatch, native root
/// bootstrap and B-tree predicate tracking remain separate. Allocation is
/// append-only: freed/rolled-back page numbers are never recycled by this owner.
/// File creation/namespace durability and the append lease are caller-owned.
pub struct NativePageStore<S: VfsFile, M: VfsFile, C: NativeObjectCodec> {
    driver: DurableWriteCoordinator<S, M, SharedCodec<C>>,
    codec: Arc<C>,
    history: PageHistory,
    owner: Arc<()>,
    active: Vec<Weak<CommitSeq>>,
    next_txn_id: u64,
    page_high_water: AtomicU64,
    recovery_required: bool,
    closed: bool,
}
impl<S: VfsFile, M: VfsFile, C: NativeObjectCodec> NativePageStore<S, M, C> {
    /// Adopt a fresh log. No user data is overwritten and no files are created.
    ///
    /// # Errors
    /// Rejects invalid bounds/page size and non-genesis or blocked logs.
    pub fn new(log: NativeDurabilityLog<S, M>, codec: C, page_size: u32, limits: NativePageLimits) -> Result<Self> {
        let history = PageHistory::new(page_size, limits)?;
        let codec = Arc::new(codec);
        let driver = DurableWriteCoordinator::new(log, SharedCodec(Arc::clone(&codec)), 1)?;
        Ok(Self {
            driver, codec, history, owner: Arc::new(()), active: Vec::new(), next_txn_id: 1,
            page_high_water: AtomicU64::new(1), // Page 1 is reserved for the database header.
            recovery_required: false, closed: false,
        })
    }

    /// Replay every bound capsule in commit order, retaining only the latest
    /// version of each page (including tombstones) between replay steps. There
    /// are no live readers in this private builder; old handles belong to a
    /// different owner. Each capsule's full read history is still checked
    /// against the exact latest stamps BEFORE replacing any heads. The exact
    /// committed-token set is retained for duplicate detection and is bounded
    /// separately by the storage record limit, not by the page-image budget.
    ///
    /// Orphan objects are not replayed. A corrupt or unserializable prefix
    /// produces no usable store. No supplied metadata or validation callback
    /// can replace the stored page images/read versions. This is bounded page
    /// replay, not streaming of the lower log's marker/index metadata, on-disk
    /// compaction, or a process-RSS bound; decoding uses its own object limits.
    ///
    /// # Errors
    /// Propagates storage/codec errors; rejects invalid history, page-size
    /// mismatches, duplicate committed tokens, and configured retention excess.
    pub async fn recover(
        cx: &Cx, symbols: S, markers: M, codec: C, page_size: u32,
        storage_limits: NativeDurabilityLimits, page_limits: NativePageLimits,
    ) -> Result<(Self, NativeDurabilityRecovery)> {
        let mut history = PageHistory::new(page_size, page_limits)?;
        let codec = Arc::new(codec);
        let (driver, report) = DurableWriteCoordinator::recover(
            cx, symbols, markers, storage_limits, SharedCodec(Arc::clone(&codec)), 1,
            |candidate, _| {
                let capsule = NativePageCapsule::from_candidate(candidate)?;
                let prepared = history.prepare(&capsule, candidate.proof.commit_seq,
                    candidate.proof.submission.txn_token, ApplyMode::Replay).map_err(|error| match error {
                        FrankenError::BusySnapshot { .. } => {
                            corrupt("unserializable committed native page history")
                        }
                        other => other,
                    })?;
                history.apply(prepared);
                Ok(())
            },
        ).await?;
        if history.tip != driver.committed_tip() {
            return Err(corrupt("native page replay/driver tip mismatch"));
        }
        let next_txn_id = history.max_txn_id.checked_add(1).ok_or(FrankenError::DatabaseFull)?;
        // Tombstones retain their page number. An old snapshot may still refer
        // to a deleted B-tree/overflow page, so recovery must not recycle it.
        let page_high_water = history.pages.last_key_value()
            .map_or(1, |(page, _)| u64::from(page.get()).max(1));
        Ok((Self {
            driver, codec, history, owner: Arc::new(()), active: Vec::new(), next_txn_id,
            page_high_water: AtomicU64::new(page_high_water),
            recovery_required: false, closed: false,
        }, report))
    }

    #[must_use]
    pub const fn committed_tip(&self) -> CommitSeq { self.history.tip }
    #[must_use]
    pub const fn page_size(&self) -> u32 { self.history.page_size }
    #[must_use]
    pub fn needs_recovery(&self) -> bool { self.recovery_required || self.driver.needs_recovery() }
    #[must_use]
    pub fn outstanding_write(&self) -> Option<VfsWriteCompletion> { self.driver.outstanding_write() }

    /// Bytes of page images retained by this store, including pinned history.
    /// This excludes private overlays and Arcs retained by callers, and is not RSS.
    #[must_use]
    pub const fn retained_page_bytes(&self) -> usize { self.history.payload_bytes }

    /// Number of retained committed page versions, including tombstones.
    #[must_use]
    pub const fn retained_version_count(&self) -> usize { self.history.version_count }

    /// Reclaim obsolete in-memory page versions in a resumable, page-budgeted
    /// sweep. Returns the number of versions removed by this call. Zero does
    /// not imply a complete sweep: the visited pages may still be pinned.
    ///
    /// Each call visits at most `page_budget` page chains, continuing after the
    /// last visited page and wrapping at the end. Cancellation is checked at
    /// page boundaries; a chain's size is bounded by `max_versions`. This is
    /// not a wall-clock pause bound. Dropped/finished transactions release their
    /// pins automatically; newly begun transactions cannot move the floor back.
    ///
    /// No files, marker records, object locators, or conflict stamps are removed.
    /// Existing returned page Arcs remain valid and may retain their allocations.
    /// Exclusive access to this owner prevents BEGIN/publication racing a sweep;
    /// it does not introduce a transaction-wide or cross-process writer lock.
    ///
    /// # Errors
    /// Rejects zero budget, cancellation, closed or indeterminate storage.
    /// On cancellation, already reclaimed pages and counters remain consistent.
    pub fn reclaim_history(&mut self, cx: &Cx, page_budget: usize) -> Result<usize> {
        self.ready(cx)?;
        if page_budget == 0 {
            return Err(FrankenError::OutOfRange {
                what: "native history page budget".to_owned(), value: "0".to_owned(),
            });
        }
        self.active.retain(|lease| lease.strong_count() != 0);
        let floor = self.active.iter().filter_map(Weak::upgrade)
            .map(|pin| *pin).min().unwrap_or(self.history.tip);
        self.history.reclaim(cx, floor, page_budget)
    }

    fn ready(&self, cx: &Cx) -> Result<()> {
        if self.closed || self.needs_recovery() { return Err(FrankenError::BusyRecovery); }
        cx.checkpoint().map_err(|_| FrankenError::Interrupt)
    }
    fn transaction(&self, cx: &Cx, txn: &NativePageTransaction) -> Result<()> {
        self.ready(cx)?;
        if !Arc::ptr_eq(&self.owner, &txn.owner) || txn.state != NativePageTransactionState::Active {
            return Err(FrankenError::Abort);
        }
        Ok(())
    }

    /// Begin without holding a transaction-wide lock or borrowing the store.
    ///
    /// # Errors
    /// Refuses unavailable storage, cancellation, exhausted tokens/session slots.
    pub fn begin(&mut self, cx: &Cx) -> Result<NativePageTransaction> {
        self.ready(cx)?;
        self.active.retain(|lease| lease.strong_count() != 0);
        if self.active.len() >= self.history.limits.max_active_transactions { return Err(FrankenError::Busy); }
        let id = TxnId::new(self.next_txn_id).ok_or(FrankenError::DatabaseFull)?;
        self.active.try_reserve(1).map_err(|_| FrankenError::OutOfMemory)?;
        let lease = Arc::new(self.history.tip);
        self.active.push(Arc::downgrade(&lease));
        self.next_txn_id += 1; // TxnId's domain is narrower than u64.
        // Preallocated, empty publication slots are not committed addresses.
        // Tombstones ARE committed addresses and remain after reclamation.
        let snapshot_extent = self.history.pages.iter().rev()
            .find(|(_, versions)| !versions.is_empty()).map_or(0, |(page, _)| page.get());
        Ok(NativePageTransaction {
            owner: Arc::clone(&self.owner), lease: Some(lease),
            token: TxnToken::new(id, TxnEpoch::new(1)), snapshot: self.history.tip,
            snapshot_extent, issued_extent: snapshot_extent, completed_extent: snapshot_extent,
            reservations: BTreeSet::new(),
            reads: BTreeMap::new(), writes: BTreeMap::new(), payload_bytes: 0,
            state: NativePageTransactionState::Active,
            savepoints: Vec::new(), next_savepoint_id: 1,
        })
    }

    fn observe(&self, txn: &mut NativePageTransaction, page: PageNumber) -> Result<()> {
        if !txn.reads.contains_key(&page) {
            overlay_size(txn.reads.len() + 1, txn.writes.len(), txn.payload_bytes)?;
            // Every retained overlay must remain representable if restored
            // after this read; rollback cannot throw away its dependencies.
            for saved in &txn.savepoints {
                overlay_size(txn.reads.len() + 1, saved.writes.len(), saved.payload_bytes)?;
            }
            let version = self.history.at(page, txn.snapshot).map_or(CommitSeq::ZERO, |v| v.seq);
            txn.reads.insert(page, version);
        }
        Ok(())
    }

    /// Read the transaction's own overlay, or the page at its fixed snapshot.
    /// An absent read is recorded just like a present page read.
    ///
    /// # Errors
    /// Rejects foreign/finished handles, cancellation and read-set limits.
    pub fn read_page(&self, cx: &Cx, txn: &mut NativePageTransaction, page: PageNumber) -> Result<Option<Arc<[u8]>>> {
        self.transaction(cx, txn)?;
        self.observe(txn, page)?;
        if let Some(data) = txn.writes.get(&page) { return Ok(data.clone()); }
        Ok(self.history.at(page, txn.snapshot).and_then(|version| version.data.clone()))
    }

    /// Buffer a complete replacement. `None` records a versioned deletion.
    /// Both are private until commit; blind writes still observe their base.
    ///
    /// # Errors
    /// Rejects foreign/finished handles, wrong page size and overlay bounds.
    pub fn write_page(&self, cx: &Cx, txn: &mut NativePageTransaction, page: PageNumber, data: Option<&[u8]>) -> Result<()> {
        self.transaction(cx, txn)?;
        let page_size = usize::try_from(self.history.page_size).map_err(|_| FrankenError::TooBig)?;
        if data.is_some_and(|bytes| bytes.len() != page_size) {
            return Err(corrupt("native page write has the wrong page size"));
        }
        let introduces_page = self.history.at(page, txn.snapshot).is_none();
        if introduces_page && !txn.reservations.contains(&page)
            && txn.reservations.len() >= MAX_CAPSULE_PAGES
        {
            return Err(FrankenError::TooBig);
        }
        let old = txn.writes.get(&page).and_then(|value| value.as_ref()).map_or(0, |value| value.len());
        let payload_bytes = txn.payload_bytes - old + data.map_or(0, |bytes| bytes.len());
        overlay_size(txn.reads.len() + usize::from(!txn.reads.contains_key(&page)),
            txn.writes.len() + usize::from(!txn.writes.contains_key(&page)), payload_bytes)?;
        self.observe(txn, page)?;
        // Explicit page writes participate in allocation, even before commit.
        // Relaxed atomics allocate distinct numbers; they do not publish pages.
        self.page_high_water.fetch_max(u64::from(page.get()), Ordering::Relaxed);
        txn.writes.insert(page, data.map(Arc::from));
        if introduces_page { txn.reservations.insert(page); }
        txn.issued_extent = txn.issued_extent.max(page.get());
        txn.payload_bytes = payload_bytes;
        Ok(())
    }

    /// Reserve a fresh page number and stage a zero-filled private page image.
    /// Allocation never edits a shared freelist page and does not take a
    /// transaction-wide lock. Distinct allocation calls get distinct numbers;
    /// explicit writes still obey ordinary snapshot/conflict validation.
    /// Gaps after failed or rolled-back allocations are not reused while this
    /// owner lives. Reopen restores the high water from committed page history.
    ///
    /// # Errors
    /// Refuses invalid handles, cancellation, page-number exhaustion, and
    /// overlay/allocation limits. No failed call publishes a page.
    pub fn allocate_page(&self, cx: &Cx, txn: &mut NativePageTransaction) -> Result<PageNumber> {
        self.transaction(cx, txn)?;
        let size = usize::try_from(self.history.page_size).map_err(|_| FrankenError::TooBig)?;
        let mut high = self.page_high_water.load(Ordering::Relaxed);
        let page = loop {
            cx.checkpoint().map_err(|_| FrankenError::Interrupt)?;
            let next = high.checked_add(1).ok_or(FrankenError::DatabaseFull)?;
            let page = u32::try_from(next).ok().and_then(PageNumber::new)
                .ok_or(FrankenError::DatabaseFull)?;
            overlay_size(
                txn.reads.len() + usize::from(!txn.reads.contains_key(&page)),
                txn.writes.len() + 1,
                txn.payload_bytes.checked_add(size)
                    .ok_or(FrankenError::TooBig)?,
            )?;
            match self.page_high_water.compare_exchange_weak(
                high, next, Ordering::Relaxed, Ordering::Relaxed,
            ) {
                Ok(_) => break page,
                Err(actual) => high = actual,
            }
        };
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(size).map_err(|_| FrankenError::OutOfMemory)?;
        bytes.resize(size, 0);
        self.write_page(cx, txn, page, Some(&bytes))?;
        Ok(page)
    }

    /// Capture a bounded private overlay without copying the page payloads.
    /// Snapshot observations are intentionally NOT rewound on rollback: a
    /// read dependency cannot disappear merely because its writes were undone.
    /// Saved payload accounting is conservative even for shared page Arcs.
    ///
    /// # Errors
    /// Rejects invalid handles, cancellation, depth/retained-overlay limits,
    /// and exhausted savepoint identifiers.
    pub fn savepoint(&self, cx: &Cx, txn: &mut NativePageTransaction) -> Result<NativePageSavepoint> {
        self.transaction(cx, txn)?;
        if txn.savepoints.len() >= MAX_NATIVE_SAVEPOINTS { return Err(FrankenError::TooBig); }
        let (writes, payload) = txn.savepoints.iter().try_fold(
            (txn.writes.len(), txn.payload_bytes),
            |(writes, payload), saved| {
                Ok::<_, FrankenError>((
                    writes.checked_add(saved.writes.len()).ok_or(FrankenError::TooBig)?,
                    payload.checked_add(saved.payload_bytes).ok_or(FrankenError::TooBig)?,
                ))
            },
        )?;
        overlay_size(0, writes, payload)?;
        let id = txn.next_savepoint_id;
        let next = id.checked_add(1).ok_or(FrankenError::TooBig)?;
        txn.savepoints.try_reserve(1).map_err(|_| FrankenError::OutOfMemory)?;
        txn.savepoints.push(SavedOverlay { id, writes: txn.writes.clone(), payload_bytes: txn.payload_bytes });
        txn.next_savepoint_id = next;
        Ok(NativePageSavepoint { owner: Arc::clone(&self.owner), token: txn.token, id })
    }

    fn savepoint_index(&self, txn: &NativePageTransaction, point: &NativePageSavepoint) -> Result<usize> {
        if !Arc::ptr_eq(&self.owner, &txn.owner) || !Arc::ptr_eq(&self.owner, &point.owner)
            || point.token != txn.token
        { return Err(FrankenError::Abort); }
        if txn.state == NativePageTransactionState::Indeterminate { return Err(FrankenError::BusyRecovery); }
        if txn.state != NativePageTransactionState::Active { return Err(FrankenError::Abort); }
        txn.savepoints.iter().position(|saved| saved.id == point.id).ok_or(FrankenError::Abort)
    }

    /// Restore the named private overlay, discard inner savepoints, and retain
    /// the named marker for another rollback. This cleanup needs no live Cx;
    /// cancellation cannot prevent undoing a private failed B-tree operation.
    /// Neither read observations nor the allocation high water are rewound.
    ///
    /// # Errors
    /// Rejects foreign, released, completed, or indeterminate markers/handles.
    pub fn rollback_to(&self, txn: &mut NativePageTransaction, point: &NativePageSavepoint) -> Result<()> {
        let index = self.savepoint_index(txn, point)?;
        let saved = &txn.savepoints[index];
        txn.writes = saved.writes.clone();
        txn.payload_bytes = saved.payload_bytes;
        txn.savepoints.truncate(index + 1);
        Ok(())
    }

    /// Release a marker and all inner markers without changing private writes.
    ///
    /// # Errors
    /// Rejects foreign, released, completed, or indeterminate markers/handles.
    pub fn release_savepoint(&self, txn: &mut NativePageTransaction, point: &NativePageSavepoint) -> Result<()> {
        let index = self.savepoint_index(txn, point)?;
        txn.savepoints.truncate(index);
        Ok(())
    }

    /// Discard only a private active overlay. No committed page is changed.
    /// Repeated rollback is harmless; an indeterminate commit cannot roll back.
    ///
    /// # Errors
    /// Rejects a foreign handle, a completed commit, or an indeterminate result.
    pub fn rollback(&self, txn: &mut NativePageTransaction) -> Result<()> {
        if !Arc::ptr_eq(&self.owner, &txn.owner) { return Err(FrankenError::Abort); }
        match txn.state {
            NativePageTransactionState::Active | NativePageTransactionState::RolledBack => {
                txn.finish(NativePageTransactionState::RolledBack); Ok(())
            }
            NativePageTransactionState::Indeterminate => Err(FrankenError::BusyRecovery),
            NativePageTransactionState::Committed => Err(FrankenError::Abort),
        }
    }

    /// Commit with mandatory page semantics, conflict checks and durable I/O.
    /// Returns `None` for a read-only transaction, which needs no new marker.
    /// A write conflict or limit failure before staging keeps the overlay active.
    /// Once staging begins, errors or dropped futures leave both the store and
    /// transaction indeterminate until recovery; no fake rollback is reported.
    ///
    /// # Errors
    /// Returns snapshot conflicts, resource limits, codec errors, or VFS errors.
    pub async fn commit(&mut self, cx: &Cx, txn: &mut NativePageTransaction, now_unix_ns: u64) -> Result<Option<DurableCommitAcknowledgement>> {
        self.transaction(cx, txn)?;
        if txn.writes.is_empty() {
            txn.finish(NativePageTransactionState::Committed);
            return Ok(None);
        }
        let capsule = NativePageCapsule {
            page_size: self.history.page_size, snapshot: txn.snapshot,
            reads: txn.reads.iter().map(|(page, seq)| (*page, *seq)).collect(),
            writes: txn.writes.iter().map(|(page, data)| NativePageWrite { page: *page, data: data.clone() }).collect(),
        };
        self.history.validate(&capsule)?;
        if self.history.projected_size(&capsule, ApplyMode::Live).is_err() {
            // Start a complete pressure sweep, including pages visited before
            // older pins were released. This happens before encoding/staging,
            // so refusal or cancellation leaves the private transaction active.
            self.history.reclaim_after = None;
            self.reclaim_history(cx, self.history.pages.len().max(1))?;
            self.history.projected_size(&capsule, ApplyMode::Live)?;
        }
        let bytes = capsule.to_bytes()?;
        let records = self.codec.encode(cx, &bytes)?;
        let object_id = records.first().ok_or_else(|| corrupt("native page encoder returned no symbols"))?.object_id;
        if self.codec.decode(cx, object_id, &records)? != bytes {
            return Err(corrupt("native page codec changed canonical capsule bytes"));
        }
        let seq = self.history.tip.get().checked_add(1).map(CommitSeq::new).ok_or(FrankenError::DatabaseFull)?;
        let prepared = self.history.prepare(&capsule, seq, txn.token, ApplyMode::Live)?;
        let submission = CommitSubmission {
            capsule_object_id: object_id, capsule_digest: *blake3::hash(&bytes).as_bytes(),
            write_set_pages: capsule.writes.iter().map(|write| write.page).collect(),
            witness_refs: Vec::new(), edge_ids: Vec::new(), merge_witness_ids: Vec::new(),
            txn_token: txn.token, begin_seq: txn.snapshot,
        };
        if cx.checkpoint().is_err() {
            // Preparation may have precreated invisible page slots. A caller
            // can retry after cancellation, so do not retain empty metadata
            // for arbitrarily many abandoned, never-committed page numbers.
            self.history.pages.retain(|_, versions| !versions.is_empty());
            return Err(FrankenError::Interrupt);
        }
        self.recovery_required = true;
        txn.state = NativePageTransactionState::Indeterminate;
        self.driver.stage_symbols(cx, &records).await?;
        let reserved = self.driver.queue(cx, submission, now_unix_ns).map_err(commit_error)?;
        if reserved != seq { return Err(corrupt("native page/coordinator reservation mismatch")); }
        let history = &self.history;
        self.driver.flush(cx, |candidates, _| {
            if candidates.len() != 1 { return Err(corrupt("unexpected native page publication batch")); }
            let decoded = NativePageCapsule::from_candidate(&candidates[0])?;
            if decoded != capsule || candidates[0].proof.commit_seq != seq {
                return Err(corrupt("native page publication changed after preparation"));
            }
            history.validate(&decoded)
        }).await?.ok_or_else(|| corrupt("native page commit produced no storage receipt"))?;
        let acknowledgement = self.driver.take_committed(seq)
            .ok_or_else(|| corrupt("native page commit produced no acknowledgement"))?;
        // All page vector/token capacity was reserved before the first write.
        self.history.apply(prepared);
        self.recovery_required = false;
        txn.finish(NativePageTransactionState::Committed);
        Ok(Some(acknowledgement))
    }

    /// Close the owned log, refusing to release it beneath a pending VFS write.
    /// Inactive caller-held snapshots do not cause a hidden commit or rollback.
    ///
    /// # Errors
    /// Propagates pending-write and VFS close errors; retry remains available.
    pub fn close(&mut self, cx: &Cx) -> Result<()> {
        self.closed = true;
        self.driver.close(cx)
    }
}

fn overlay_size(reads: usize, writes: usize, payload: usize) -> Result<()> {
    if reads > MAX_CAPSULE_PAGES || writes > MAX_CAPSULE_PAGES
        || reads.checked_mul(12).and_then(|n| writes.checked_mul(8).and_then(|m| n.checked_add(m)))
            .and_then(|n| n.checked_add(28)).and_then(|n| n.checked_add(payload))
            .is_none_or(|n| n > MAX_PAGE_CAPSULE_BYTES)
    {
        return Err(FrankenError::TooBig);
    }
    Ok(())
}
fn commit_error(error: DurableCommitError) -> FrankenError {
    match error {
        DurableCommitError::Failure(error) => error,
        DurableCommitError::Rejected(CommitResult::ConflictFcw { conflicting_pages }) => {
            FrankenError::BusySnapshot { conflicting_pages: format!("{conflicting_pages:?}") }
        }
        DurableCommitError::Rejected(CommitResult::ConflictSsi) => {
            FrankenError::BusySnapshot { conflicting_pages: "native validation".to_owned() }
        }
        DurableCommitError::Rejected(_) => FrankenError::Abort,
    }
}

#[cfg(test)]
mod allocation_savepoint_tests {
    use super::*;
    use fsqlite_types::{Oti, SymbolRecordFlags, reconstruct_systematic_happy_path};
    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_vfs::{MemoryVfs, Vfs};
    use crate::test_support::FutureResultTestExt;

    struct TestCodec;
    impl NativeObjectCodec for TestCodec {
        fn encode(&self, _: &Cx, bytes: &[u8]) -> Result<Vec<SymbolRecord>> {
            let size = u32::try_from(bytes.len()).map_err(|_| FrankenError::TooBig)?;
            Ok(vec![SymbolRecord::new(ObjectId::derive_from_canonical_bytes(bytes),
                Oti { f: u64::from(size), al: 1, t: size, z: 1, n: 1 }, 0,
                bytes.to_vec(), SymbolRecordFlags::SYSTEMATIC_RUN_START)])
        }
        fn decode(&self, _: &Cx, id: ObjectId, records: &[SymbolRecord]) -> Result<Vec<u8>> {
            let bytes = reconstruct_systematic_happy_path(records)
                .map_err(|error| corrupt(&error.to_string()))?;
            if ObjectId::derive_from_canonical_bytes(&bytes) != id {
                return Err(corrupt("test capsule identity mismatch"));
            }
            Ok(bytes)
        }
    }
    type Store = NativePageStore<<MemoryVfs as Vfs>::File, <MemoryVfs as Vfs>::File, TestCodec>;
    fn file(vfs: &MemoryVfs, cx: &Cx, name: &str) -> <MemoryVfs as Vfs>::File {
        vfs.open(cx, Some(std::path::Path::new(name)),
            VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL).unwrap().0
    }
    fn store(vfs: &MemoryVfs, cx: &Cx) -> Store {
        let log = NativeDurabilityLog::create(cx, file(vfs, cx, "objects"), file(vfs, cx, "markers"),
            NativeDurabilityLimits::default()).unwrap();
        NativePageStore::new(log, TestCodec, 512, NativePageLimits::default()).unwrap()
    }
    fn page(n: u32) -> PageNumber { PageNumber::new(n).unwrap() }

    #[test]
    fn native_extent_is_pinned_before_reads_and_excludes_peer_reservations() {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        let mut old = db.begin(&cx).unwrap();
        let mut private = db.begin(&cx).unwrap();
        db.write_page(&cx, &mut private, page(900), Some(&[9; 512])).unwrap();
        let mut before = db.begin(&cx).unwrap();
        assert_eq!(before.snapshot_db_size(), 0);
        assert_eq!(before.visible_db_size_bound(), 0);
        let mut peer = db.begin(&cx).unwrap();
        db.write_page(&cx, &mut peer, page(7), Some(&[7; 512])).unwrap();
        db.commit(&cx, &mut peer, 1).expect("publish smaller peer address");
        assert_eq!(peer.snapshot_db_size(), 0);
        assert_eq!(peer.live_db_size(), 7);
        assert_eq!(old.snapshot_db_size(), 0);
        assert_eq!(old.live_db_size(), 0);
        assert!(db.read_page(&cx, &mut old, page(7)).unwrap().is_none());
        let mut fresh = db.begin(&cx).unwrap();
        assert_eq!(fresh.snapshot_db_size(), 7);
        assert_eq!(private.live_db_size(), 900);
        db.rollback(&mut private).unwrap();
        assert_eq!(private.live_db_size(), 0);
        assert!(private.live_reserved_pages().is_empty());
        db.rollback(&mut old).unwrap(); db.rollback(&mut before).unwrap();
        db.rollback(&mut fresh).unwrap(); db.close(&cx).unwrap();
    }

    #[test]
    fn native_extent_savepoints_preserve_spent_addresses_but_not_discarded_growth() {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        let mut txn = db.begin(&cx).unwrap();
        db.write_page(&cx, &mut txn, page(2), Some(&[2; 512])).unwrap();
        let point = db.savepoint(&cx, &mut txn).unwrap();
        db.write_page(&cx, &mut txn, page(100), Some(&[1; 512])).unwrap();
        let allocated = db.allocate_page(&cx, &mut txn).unwrap();
        assert_eq!(allocated, page(101));
        db.rollback_to(&mut txn, &point).unwrap();
        assert_eq!(txn.snapshot_db_size(), 0);
        assert_eq!(txn.live_db_size(), 2);
        assert_eq!(txn.visible_db_size_bound(), 101);
        assert_eq!(txn.live_reserved_pages(), vec![page(2), page(100), page(101)]);
        assert!(db.read_page(&cx, &mut txn, allocated).unwrap().is_none());
        db.commit(&cx, &mut txn, 1).expect("only surviving growth commits");
        assert_eq!(txn.live_db_size(), 2);
        assert!(txn.live_reserved_pages().is_empty());
        let mut fresh = db.begin(&cx).unwrap();
        assert_eq!(fresh.snapshot_db_size(), 2);
        assert_eq!(db.allocate_page(&cx, &mut fresh).unwrap(), page(102));
        db.rollback(&mut fresh).unwrap(); db.close(&cx).unwrap();
    }

    #[test]
    fn native_extent_tombstones_survive_gc_and_recovery_without_dense_hole_lists() {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        let mut txn = db.begin(&cx).unwrap();
        db.write_page(&cx, &mut txn, page(1_000_000), Some(&[1; 512])).unwrap();
        assert_eq!(txn.live_reserved_pages(), vec![page(1_000_000)]);
        db.commit(&cx, &mut txn, 1).expect("sparse native extent");
        let mut delete = db.begin(&cx).unwrap();
        db.write_page(&cx, &mut delete, page(1_000_000), None).unwrap();
        assert!(delete.live_reserved_pages().is_empty());
        db.commit(&cx, &mut delete, 2).expect("retire high address");
        db.reclaim_history(&cx, 10).unwrap();
        let mut empty = db.begin(&cx).unwrap();
        assert_eq!(empty.snapshot_db_size(), 1_000_000);
        assert!(db.read_page(&cx, &mut empty, page(1_000_000)).unwrap().is_none());
        db.rollback(&mut empty).unwrap(); db.close(&cx).unwrap();
        let (mut recovered, _) = Store::recover(&cx, file(&vfs, &cx, "objects"),
            file(&vfs, &cx, "markers"), TestCodec, 512, NativeDurabilityLimits::default(),
            NativePageLimits::default()).expect("recover sparse tombstone extent");
        let mut next = recovered.begin(&cx).unwrap();
        assert_eq!(next.snapshot_db_size(), 1_000_000);
        assert_eq!(next.live_db_size(), 1_000_000);
        assert!(next.live_reserved_pages().is_empty());
        assert_eq!(recovered.allocate_page(&cx, &mut next).unwrap(), page(1_000_001));
        recovered.rollback(&mut next).unwrap(); recovered.close(&cx).unwrap();
    }

    #[test]
    fn native_extent_ignores_unpublished_preparation_slots() {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        db.history.pages.insert(page(900), Vec::new());
        let mut txn = db.begin(&cx).unwrap();
        assert_eq!(txn.snapshot_db_size(), 0);
        assert!(db.write_page(&cx, &mut txn, page(99), Some(&[0; 511])).is_err());
        assert_eq!(txn.live_db_size(), 0);
        assert_eq!(txn.visible_db_size_bound(), 0);
        assert!(txn.live_reserved_pages().is_empty());
        db.write_page(&cx, &mut txn, page(3), Some(&[3; 512])).unwrap();
        db.commit(&cx, &mut txn, 1).expect("actual page, not capacity slot, publishes");
        let mut next = db.begin(&cx).unwrap();
        assert_eq!(next.snapshot_db_size(), 3);
        db.rollback(&mut next).unwrap(); db.close(&cx).unwrap();
    }

    #[test]
    fn allocations_are_private_and_distinct_across_open_transactions() {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        let mut a = db.begin(&cx).unwrap(); let mut b = db.begin(&cx).unwrap();
        let pa = db.allocate_page(&cx, &mut a).unwrap();
        let pb = db.allocate_page(&cx, &mut b).unwrap();
        assert_eq!(pa, page(2)); assert_eq!(pb, page(3));
        assert_eq!(db.page_size(), 512);
        assert_eq!(db.read_page(&cx, &mut a, pa).unwrap().unwrap().as_ref(), &[0; 512]);
        let mut old = db.begin(&cx).unwrap();
        assert!(db.read_page(&cx, &mut old, pa).unwrap().is_none());
        db.commit(&cx, &mut b, 100).expect("disjoint writer B commits first");
        db.commit(&cx, &mut a, 101).expect("disjoint writer A commits second");
        assert!(db.read_page(&cx, &mut old, pb).unwrap().is_none());
        db.rollback(&mut old).unwrap(); db.close(&cx).unwrap();
    }

    #[test]
    fn direct_writes_rollback_and_tombstones_do_not_recycle_page_numbers() {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        let mut first = db.begin(&cx).unwrap();
        db.write_page(&cx, &mut first, page(20), Some(&[9; 512])).unwrap();
        let mut second = db.begin(&cx).unwrap();
        assert_eq!(db.allocate_page(&cx, &mut second).unwrap(), page(21));
        db.rollback(&mut first).unwrap(); db.rollback(&mut second).unwrap();
        let mut third = db.begin(&cx).unwrap();
        let p = db.allocate_page(&cx, &mut third).unwrap();
        assert_eq!(p, page(22));
        db.write_page(&cx, &mut third, p, None).unwrap();
        db.commit(&cx, &mut third, 100).expect("persist deleted allocation");
        db.close(&cx).unwrap();
        let (mut reopened, _) = Store::recover(&cx, file(&vfs, &cx, "objects"),
            file(&vfs, &cx, "markers"), TestCodec, 512, NativeDurabilityLimits::default(),
            NativePageLimits::default()).expect("recover allocation high water");
        let mut next = reopened.begin(&cx).unwrap();
        assert_eq!(reopened.allocate_page(&cx, &mut next).unwrap(), page(23));
        reopened.rollback(&mut next).unwrap(); reopened.close(&cx).unwrap();
    }

    #[test]
    fn allocation_exhaustion_does_not_wrap_or_publish() {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        let mut txn = db.begin(&cx).unwrap();
        db.page_high_water.store(u64::from(u32::MAX), Ordering::Relaxed);
        assert!(matches!(db.allocate_page(&cx, &mut txn), Err(FrankenError::DatabaseFull)));
        assert!(txn.writes.is_empty()); assert_eq!(db.committed_tip(), CommitSeq::ZERO);
        db.rollback(&mut txn).unwrap(); db.close(&cx).unwrap();
    }

    #[test]
    fn nested_savepoints_restore_images_tombstones_and_allocations() {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        let mut txn = db.begin(&cx).unwrap();
        let p = db.allocate_page(&cx, &mut txn).unwrap();
        db.write_page(&cx, &mut txn, p, Some(&[1; 512])).unwrap();
        let outer = db.savepoint(&cx, &mut txn).unwrap();
        db.write_page(&cx, &mut txn, p, None).unwrap();
        let inner = db.savepoint(&cx, &mut txn).unwrap();
        let abandoned = db.allocate_page(&cx, &mut txn).unwrap();
        db.rollback_to(&mut txn, &inner).unwrap();
        assert!(db.read_page(&cx, &mut txn, p).unwrap().is_none());
        assert!(db.read_page(&cx, &mut txn, abandoned).unwrap().is_none());
        assert!(db.allocate_page(&cx, &mut txn).unwrap() > abandoned);
        db.rollback_to(&mut txn, &outer).unwrap();
        assert!(db.rollback_to(&mut txn, &inner).is_err());
        assert_eq!(db.read_page(&cx, &mut txn, p).unwrap().unwrap().as_ref(), &[1; 512]);
        db.write_page(&cx, &mut txn, p, Some(&[2; 512])).unwrap();
        db.rollback_to(&mut txn, &outer).unwrap(); // Named marker remains usable.
        db.release_savepoint(&mut txn, &outer).unwrap();
        assert!(db.rollback_to(&mut txn, &outer).is_err());
        db.commit(&cx, &mut txn, 100).expect("commit restored image");
        let mut fresh = db.begin(&cx).unwrap();
        assert_eq!(db.read_page(&cx, &mut fresh, p).unwrap().unwrap().as_ref(), &[1; 512]);
        assert!(db.read_page(&cx, &mut fresh, abandoned).unwrap().is_none());
        db.rollback(&mut fresh).unwrap(); db.close(&cx).unwrap();
    }

    #[test]
    fn rollback_to_retains_read_dependencies_and_cannot_hide_write_skew() {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        let mut a = db.begin(&cx).unwrap(); let point = db.savepoint(&cx, &mut a).unwrap();
        assert!(db.read_page(&cx, &mut a, page(9)).unwrap().is_none());
        let mut b = db.begin(&cx).unwrap();
        db.write_page(&cx, &mut b, page(9), Some(&[1; 512])).unwrap();
        db.commit(&cx, &mut b, 100).expect("other writer commits");
        db.rollback_to(&mut a, &point).unwrap();
        db.write_page(&cx, &mut a, page(10), Some(&[2; 512])).unwrap();
        assert!(matches!(db.commit(&cx, &mut a, 101).wait(), Err(FrankenError::BusySnapshot { .. })));
        assert_eq!(a.state(), NativePageTransactionState::Active);
        db.rollback(&mut a).unwrap(); db.close(&cx).unwrap();
    }

    #[test]
    fn savepoint_cleanup_is_cancel_safe_and_rejects_foreign_or_finished_handles() {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        let mut a = db.begin(&cx).unwrap(); let mut b = db.begin(&cx).unwrap();
        let point = db.savepoint(&cx, &mut a).unwrap();
        assert!(db.rollback_to(&mut b, &point).is_err());
        db.allocate_page(&cx, &mut a).unwrap();
        cx.cancel();
        assert!(db.savepoint(&cx, &mut a).is_err());
        db.rollback_to(&mut a, &point).unwrap();
        assert!(a.writes.is_empty());
        a.state = NativePageTransactionState::Indeterminate;
        assert!(matches!(db.rollback_to(&mut a, &point), Err(FrankenError::BusyRecovery)));
        a.state = NativePageTransactionState::Active; // No I/O occurred in this test.
        db.rollback(&mut a).unwrap();
        assert!(db.release_savepoint(&mut a, &point).is_err());
        db.rollback(&mut b).unwrap(); db.close(&Cx::new()).unwrap();
    }

    #[test]
    fn releasing_nested_savepoints_keeps_writes_and_bounds_retention() {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        let mut txn = db.begin(&cx).unwrap();
        let outer = db.savepoint(&cx, &mut txn).unwrap();
        let inner = db.savepoint(&cx, &mut txn).unwrap();
        let p = db.allocate_page(&cx, &mut txn).unwrap();
        db.release_savepoint(&mut txn, &outer).unwrap();
        assert!(txn.is_page_dirty(p));
        assert!(db.release_savepoint(&mut txn, &inner).is_err());
        for _ in 0..MAX_NATIVE_SAVEPOINTS { db.savepoint(&cx, &mut txn).unwrap(); }
        assert!(matches!(db.savepoint(&cx, &mut txn), Err(FrankenError::TooBig)));
        assert!(txn.is_page_dirty(p));
        db.rollback(&mut txn).unwrap();
        assert!(txn.savepoints.is_empty()); db.close(&cx).unwrap();
    }

    fn put(db: &mut Store, cx: &Cx, p: u32, data: Option<&[u8]>, time: u64) {
        let mut txn = db.begin(cx).unwrap();
        db.write_page(cx, &mut txn, page(p), data).unwrap();
        db.commit(cx, &mut txn, time).expect("publish page version");
    }

    #[test]
    fn native_gc_pins_unread_snapshots_and_keeps_each_required_floor() {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        put(&mut db, &cx, 9, Some(&[1; 512]), 1);
        let mut oldest = db.begin(&cx).unwrap(); // Deliberately do not read yet.
        put(&mut db, &cx, 9, Some(&[2; 512]), 2);
        let mut middle = db.begin(&cx).unwrap();
        put(&mut db, &cx, 9, Some(&[3; 512]), 3);
        assert_eq!(db.reclaim_history(&cx, 1).unwrap(), 0);
        assert_eq!(db.retained_version_count(), 3);
        let held = db.read_page(&cx, &mut oldest, page(9)).unwrap().unwrap();
        assert_eq!(held.as_ref(), &[1; 512]);
        db.rollback(&mut oldest).unwrap();
        assert_eq!(db.reclaim_history(&cx, 1).unwrap(), 1);
        assert_eq!(db.retained_page_bytes(), 1024);
        assert_eq!(db.read_page(&cx, &mut middle, page(9)).unwrap().unwrap().as_ref(), &[2; 512]);
        drop(middle); // Dropping, not only explicit rollback, releases a pin.
        assert_eq!(db.reclaim_history(&cx, 1).unwrap(), 1);
        assert_eq!(db.retained_version_count(), 1);
        assert_eq!(db.retained_page_bytes(), 512);
        assert_eq!(held.as_ref(), &[1; 512], "caller-owned Arc must remain valid");
        assert_eq!(db.history.latest(page(9)), CommitSeq::new(3));
        db.close(&cx).unwrap();
    }

    #[test]
    fn native_gc_keeps_old_absence_and_latest_deletion_stamps() {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        let mut absent = db.begin(&cx).unwrap();
        put(&mut db, &cx, 19, Some(&[7; 512]), 1);
        put(&mut db, &cx, 19, None, 2);
        assert_eq!(db.reclaim_history(&cx, 10).unwrap(), 0);
        assert!(db.read_page(&cx, &mut absent, page(19)).unwrap().is_none());
        db.write_page(&cx, &mut absent, page(20), Some(&[8; 512])).unwrap();
        assert!(matches!(db.commit(&cx, &mut absent, 3).wait(),
            Err(FrankenError::BusySnapshot { .. })));
        db.rollback(&mut absent).unwrap();
        assert_eq!(db.reclaim_history(&cx, 10).unwrap(), 1);
        assert_eq!(db.retained_page_bytes(), 0);
        assert_eq!(db.retained_version_count(), 1);
        assert_eq!(db.history.latest(page(19)), CommitSeq::new(2));
        let mut next = db.begin(&cx).unwrap();
        assert!(db.read_page(&cx, &mut next, page(19)).unwrap().is_none());
        assert_eq!(next.reads[&page(19)], CommitSeq::new(2));
        assert!(db.allocate_page(&cx, &mut next).unwrap() > page(20));
        db.rollback(&mut next).unwrap(); db.close(&cx).unwrap();
    }

    #[test]
    fn native_gc_sweep_resumes_with_a_budget_and_does_not_change_storage() {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        for p in [2, 4, 6] {
            put(&mut db, &cx, p, Some(&[1; 512]), 1);
            put(&mut db, &cx, p, Some(&[2; 512]), 2);
        }
        let mut objects = file(&vfs, &cx, "objects");
        let mut markers = file(&vfs, &cx, "markers");
        let lengths = (objects.file_size(&cx).unwrap(), markers.file_size(&cx).unwrap());
        for remaining in [5, 4, 3] {
            assert_eq!(db.reclaim_history(&cx, 1).unwrap(), 1);
            assert_eq!(db.retained_version_count(), remaining);
        }
        assert!(db.history.reclaim_after.is_none());
        assert_eq!(db.reclaim_history(&cx, 3).unwrap(), 0);
        assert_eq!(db.retained_page_bytes(), 1536);
        assert_eq!(lengths, (objects.file_size(&cx).unwrap(), markers.file_size(&cx).unwrap()));
        assert_eq!(db.committed_tip(), CommitSeq::new(6));
        assert_eq!(db.history.tokens.len(), 6, "GC must not erase replay identities");
        objects.close(&cx).unwrap(); markers.close(&cx).unwrap(); db.close(&cx).unwrap();
    }

    #[test]
    fn native_gc_retains_a_page_floor_older_than_the_global_snapshot() {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        put(&mut db, &cx, 2, Some(&[1; 512]), 1);
        put(&mut db, &cx, 3, Some(&[2; 512]), 2);
        put(&mut db, &cx, 3, Some(&[3; 512]), 3);
        let mut pinned = db.begin(&cx).unwrap(); // Global floor 3; page 2 floor 1.
        put(&mut db, &cx, 2, Some(&[4; 512]), 4);
        assert_eq!(db.reclaim_history(&cx, 10).unwrap(), 1);
        assert_eq!(db.history.pages[&page(2)].len(), 2);
        assert_eq!(db.read_page(&cx, &mut pinned, page(2)).unwrap().unwrap().as_ref(), &[1; 512]);
        assert_eq!(db.read_page(&cx, &mut pinned, page(3)).unwrap().unwrap().as_ref(), &[3; 512]);
        db.rollback(&mut pinned).unwrap(); db.close(&cx).unwrap();
    }

    #[test]
    fn native_gc_pressure_allows_repeated_updates_without_raising_page_limits() {
        let cx = Cx::new(); let vfs = MemoryVfs::new();
        let log = NativeDurabilityLog::create(&cx, file(&vfs, &cx, "objects"),
            file(&vfs, &cx, "markers"), NativeDurabilityLimits::default()).unwrap();
        let limits = NativePageLimits {
            max_retained_page_bytes: 1024, max_versions: 2, ..NativePageLimits::default()
        };
        let mut db = NativePageStore::new(log, TestCodec, 512, limits).unwrap();
        for value in 1_u8..=64 {
            put(&mut db, &cx, 7, Some(&[value; 512]), u64::from(value));
            assert_eq!(db.committed_tip(), CommitSeq::new(u64::from(value)));
            assert!(db.retained_page_bytes() <= 1024);
            assert!(db.retained_version_count() <= 2);
            assert!(!db.needs_recovery());
        }
        db.reclaim_history(&cx, 1).unwrap();
        assert_eq!(db.retained_page_bytes(), 512);
        let mut view = db.begin(&cx).unwrap();
        assert_eq!(db.read_page(&cx, &mut view, page(7)).unwrap().unwrap().as_ref(), &[64; 512]);
        db.rollback(&mut view).unwrap(); db.close(&cx).unwrap();
    }

    #[test]
    fn native_gc_pressure_refuses_pinned_excess_and_allows_the_same_txn_to_retry() {
        let cx = Cx::new(); let vfs = MemoryVfs::new();
        let log = NativeDurabilityLog::create(&cx, file(&vfs, &cx, "objects"),
            file(&vfs, &cx, "markers"), NativeDurabilityLimits::default()).unwrap();
        let limits = NativePageLimits {
            max_retained_page_bytes: 1024, max_versions: 2, ..NativePageLimits::default()
        };
        let mut db = NativePageStore::new(log, TestCodec, 512, limits).unwrap();
        put(&mut db, &cx, 7, Some(&[1; 512]), 1);
        let mut old = db.begin(&cx).unwrap();
        put(&mut db, &cx, 7, Some(&[2; 512]), 2);
        let mut retry = db.begin(&cx).unwrap();
        db.write_page(&cx, &mut retry, page(7), Some(&[3; 512])).unwrap();
        let mut objects = file(&vfs, &cx, "objects");
        let before = objects.file_size(&cx).unwrap();
        assert!(matches!(db.commit(&cx, &mut retry, 3).wait(), Err(FrankenError::TooBig)));
        assert_eq!(objects.file_size(&cx).unwrap(), before);
        assert_eq!(retry.state(), NativePageTransactionState::Active);
        assert!(!db.needs_recovery());
        assert_eq!(db.read_page(&cx, &mut old, page(7)).unwrap().unwrap().as_ref(), &[1; 512]);
        db.rollback(&mut old).unwrap();
        db.commit(&cx, &mut retry, 3).expect("same overlay fits after old reader exits");
        assert_eq!(db.committed_tip(), CommitSeq::new(3));
        objects.close(&cx).unwrap(); db.close(&cx).unwrap();
    }

    #[test]
    fn native_gc_cancellation_and_indeterminate_state_do_not_authorize_reclamation() {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        put(&mut db, &cx, 2, Some(&[1; 512]), 1);
        put(&mut db, &cx, 2, Some(&[2; 512]), 2);
        assert!(db.reclaim_history(&cx, 0).is_err());
        let cancelled = Cx::new(); cancelled.cancel();
        assert!(matches!(db.reclaim_history(&cancelled, 1), Err(FrankenError::Interrupt)));
        assert_eq!(db.retained_version_count(), 2);
        db.recovery_required = true; // State-boundary unit test; no uncertain I/O.
        assert!(matches!(db.reclaim_history(&cx, 1), Err(FrankenError::BusyRecovery)));
        assert_eq!(db.retained_version_count(), 2);
        db.recovery_required = false;
        assert_eq!(db.reclaim_history(&cx, 1).unwrap(), 1);
        db.close(&cx).unwrap();
        assert!(db.reclaim_history(&cx, 1).is_err());
    }

    #[test]
    fn native_gc_does_not_erase_reads_or_saved_overlay_dependencies() {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        put(&mut db, &cx, 2, Some(&[1; 512]), 1);
        let mut old = db.begin(&cx).unwrap();
        let point = db.savepoint(&cx, &mut old).unwrap();
        db.write_page(&cx, &mut old, page(2), Some(&[9; 512])).unwrap();
        put(&mut db, &cx, 2, Some(&[2; 512]), 2);
        assert_eq!(db.reclaim_history(&cx, 10).unwrap(), 0);
        db.rollback_to(&mut old, &point).unwrap();
        assert_eq!(db.read_page(&cx, &mut old, page(2)).unwrap().unwrap().as_ref(), &[1; 512]);
        db.write_page(&cx, &mut old, page(3), Some(&[3; 512])).unwrap();
        assert!(matches!(db.commit(&cx, &mut old, 3).wait(), Err(FrankenError::BusySnapshot { .. })));
        db.rollback(&mut old).unwrap();
        assert_eq!(db.reclaim_history(&cx, 10).unwrap(), 1);
        db.close(&cx).unwrap();
    }

    #[test]
    fn native_replay_reopens_reclaimed_history_under_the_original_page_budget() {
        let cx = Cx::new(); let vfs = MemoryVfs::new();
        let limits = NativePageLimits {
            max_retained_page_bytes: 1024, max_versions: 2, ..NativePageLimits::default()
        };
        let log = NativeDurabilityLog::create(&cx, file(&vfs, &cx, "objects"),
            file(&vfs, &cx, "markers"), NativeDurabilityLimits::default()).unwrap();
        let mut db = NativePageStore::new(log, TestCodec, 512, limits).unwrap();
        for value in 1_u8..=64 {
            put(&mut db, &cx, 7, Some(&[value; 512]), u64::from(value));
        }
        let mut foreign = db.begin(&cx).unwrap();
        db.close(&cx).unwrap();
        let (mut recovered, report) = Store::recover(&cx, file(&vfs, &cx, "objects"),
            file(&vfs, &cx, "markers"), TestCodec, 512, NativeDurabilityLimits::default(),
            limits).expect("replay must not materialize all 64 historical images");
        assert_eq!(report.markers.len(), 64);
        assert_eq!(recovered.retained_page_bytes(), 512);
        assert_eq!(recovered.retained_version_count(), 1);
        assert!(recovered.read_page(&cx, &mut foreign, page(7)).is_err());
        let mut next = recovered.begin(&cx).unwrap();
        assert_eq!(next.token().id.get(), 65);
        assert_eq!(recovered.read_page(&cx, &mut next, page(7)).unwrap().unwrap().as_ref(), &[64; 512]);
        recovered.write_page(&cx, &mut next, page(7), Some(&[65; 512])).unwrap();
        let ack = recovered.commit(&cx, &mut next, 0).expect("append after bounded replay").unwrap();
        assert_eq!(ack.commit_seq, CommitSeq::new(65));
        assert_eq!(ack.commit_time_unix_ns, 65);
        recovered.close(&cx).unwrap();
        let (mut again, report) = Store::recover(&cx, file(&vfs, &cx, "objects"),
            file(&vfs, &cx, "markers"), TestCodec, 512, NativeDurabilityLimits::default(),
            limits).expect("replay extended chain");
        assert_eq!(report.markers.len(), 65);
        assert_eq!(again.retained_version_count(), 1);
        let mut view = again.begin(&cx).unwrap();
        assert_eq!(again.read_page(&cx, &mut view, page(7)).unwrap().unwrap().as_ref(), &[65; 512]);
        again.rollback(&mut view).unwrap(); again.close(&cx).unwrap();
    }

    #[test]
    fn native_replay_keeps_tombstones_allocation_high_water_and_exact_tokens() {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        put(&mut db, &cx, 99, Some(&[1; 512]), 1);
        put(&mut db, &cx, 99, Some(&[2; 512]), 2);
        put(&mut db, &cx, 99, None, 3);
        db.close(&cx).unwrap();
        let limits = NativePageLimits {
            max_retained_page_bytes: 512, max_versions: 1, ..NativePageLimits::default()
        };
        let (mut recovered, _) = Store::recover(&cx, file(&vfs, &cx, "objects"),
            file(&vfs, &cx, "markers"), TestCodec, 512, NativeDurabilityLimits::default(),
            limits).expect("retain only the latest tombstone");
        assert_eq!(recovered.retained_page_bytes(), 0);
        assert_eq!(recovered.retained_version_count(), 1);
        assert_eq!(recovered.history.tokens.len(), 3);
        assert_eq!(recovered.history.latest(page(99)), CommitSeq::new(3));
        let mut view = recovered.begin(&cx).unwrap();
        assert_eq!(view.token().id.get(), 4);
        assert!(recovered.read_page(&cx, &mut view, page(99)).unwrap().is_none());
        assert_eq!(view.reads[&page(99)], CommitSeq::new(3));
        assert_eq!(recovered.allocate_page(&cx, &mut view).unwrap(), page(100));
        recovered.rollback(&mut view).unwrap(); recovered.close(&cx).unwrap();
    }

    fn adversarial_replay_log(vfs: &MemoryVfs, cx: &Cx, duplicate_token: bool) {
        use crate::native_commit::durable::NativeCommitProof;
        use fsqlite_types::CommitMarker;

        // Produce envelope-valid but semantically invalid committed inputs
        // through the lower storage layer. No page-store validator is bypassed
        // by the tested recovery entrypoint; it must reject this forged history.
        let mut log = NativeDurabilityLog::create(cx, file(vfs, cx, "objects"),
            file(vfs, cx, "markers"), NativeDurabilityLimits::default()).unwrap();
        let mut previous = None;
        for seq in 1_u64..=3 {
            let observed = if seq == 3 && !duplicate_token { 1 } else { seq - 1 };
            let capsule = NativePageCapsule {
                page_size: 512, snapshot: CommitSeq::new(seq - 1),
                reads: vec![(page(2), CommitSeq::new(observed))],
                writes: vec![NativePageWrite {
                    page: page(2), data: Some(Arc::from(vec![u8::try_from(seq).unwrap(); 512])),
                }],
            };
            let bytes = capsule.to_bytes().unwrap();
            let capsule_records = TestCodec.encode(cx, &bytes).unwrap();
            let capsule_id = capsule_records[0].object_id;
            let token_id = if seq == 3 && duplicate_token { 1 } else { seq };
            let proof = NativeCommitProof {
                commit_seq: CommitSeq::new(seq), commit_time_unix_ns: seq,
                submission: CommitSubmission {
                    capsule_object_id: capsule_id, capsule_digest: *blake3::hash(&bytes).as_bytes(),
                    write_set_pages: vec![page(2)], witness_refs: vec![], edge_ids: vec![],
                    merge_witness_ids: vec![], begin_seq: capsule.snapshot,
                    txn_token: TxnToken::new(TxnId::new(token_id).unwrap(), TxnEpoch::new(1)),
                },
            };
            let proof_records = TestCodec.encode(cx, &proof.to_bytes().unwrap()).unwrap();
            let marker = CommitMarker::new(CommitSeq::new(seq), seq, capsule_id,
                proof_records[0].object_id, previous);
            log.append_symbols(cx, &capsule_records).expect("stage adversarial capsule");
            log.append_symbols(cx, &proof_records).expect("stage bound proof");
            log.publish(cx, std::slice::from_ref(&marker), |id, records| {
                std::future::ready(TestCodec.decode(cx, id, &records).map(|_| ()))
            }).expect("lower log checks object bytes, not page semantics");
            previous = Some(ObjectId::derive_from_canonical_bytes(&marker.to_record_bytes()));
        }
        log.close(cx).unwrap();
    }

    #[test]
    fn native_replay_rejects_duplicate_tokens_after_replacing_their_page_images() {
        let cx = Cx::new(); let vfs = MemoryVfs::new();
        adversarial_replay_log(&vfs, &cx, true);
        let limits = NativePageLimits {
            max_retained_page_bytes: 512, max_versions: 1, ..NativePageLimits::default()
        };
        let result = Store::recover(&cx, file(&vfs, &cx, "objects"),
            file(&vfs, &cx, "markers"), TestCodec, 512, NativeDurabilityLimits::default(),
            limits).wait();
        assert!(matches!(result, Err(FrankenError::WalCorrupt { detail })
            if detail.contains("duplicate native page commit")));
    }

    #[test]
    fn native_replay_rejects_stale_observations_after_replacing_old_images() {
        let cx = Cx::new(); let vfs = MemoryVfs::new();
        adversarial_replay_log(&vfs, &cx, false);
        let limits = NativePageLimits {
            max_retained_page_bytes: 512, max_versions: 1, ..NativePageLimits::default()
        };
        let result = Store::recover(&cx, file(&vfs, &cx, "objects"),
            file(&vfs, &cx, "markers"), TestCodec, 512, NativeDurabilityLimits::default(),
            limits).wait();
        assert!(matches!(result, Err(FrankenError::WalCorrupt { detail })
            if detail.contains("unserializable committed native page history")));
    }

    #[test]
    fn native_replay_budgets_the_whole_commit_before_applying_any_mutation() {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        put(&mut db, &cx, 9, Some(&[1; 512]), 1);
        let mut txn = db.begin(&cx).unwrap();
        // The insertion sorts BEFORE the deletion. Replay must account for
        // both rather than reject a transient double-image total mid-commit.
        db.write_page(&cx, &mut txn, page(2), Some(&[2; 512])).unwrap();
        db.write_page(&cx, &mut txn, page(9), None).unwrap();
        db.commit(&cx, &mut txn, 2).expect("commit atomic page replacement");
        db.close(&cx).unwrap();
        let limits = NativePageLimits {
            max_retained_page_bytes: 512, max_versions: 2, ..NativePageLimits::default()
        };
        let (mut recovered, _) = Store::recover(&cx, file(&vfs, &cx, "objects"),
            file(&vfs, &cx, "markers"), TestCodec, 512, NativeDurabilityLimits::default(),
            limits).expect("net live page images fit the budget");
        assert_eq!(recovered.retained_page_bytes(), 512);
        assert_eq!(recovered.retained_version_count(), 2);
        let mut view = recovered.begin(&cx).unwrap();
        assert_eq!(recovered.read_page(&cx, &mut view, page(2)).unwrap().unwrap().as_ref(), &[2; 512]);
        assert!(recovered.read_page(&cx, &mut view, page(9)).unwrap().is_none());
        assert_eq!(view.reads[&page(9)], CommitSeq::new(2));
        recovered.rollback(&mut view).unwrap(); recovered.close(&cx).unwrap();
    }

    #[test]
    fn native_replay_does_not_weaken_limits_for_distinct_current_pages() {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        let mut txn = db.begin(&cx).unwrap();
        for p in [2, 3] { db.write_page(&cx, &mut txn, page(p), Some(&[1; 512])).unwrap(); }
        db.commit(&cx, &mut txn, 1).expect("two current pages"); db.close(&cx).unwrap();
        for (bytes, versions) in [(512, 2), (1024, 1)] {
            let limits = NativePageLimits {
                max_retained_page_bytes: bytes, max_versions: versions, ..NativePageLimits::default()
            };
            assert!(matches!(Store::recover(&cx, file(&vfs, &cx, "objects"),
                file(&vfs, &cx, "markers"), TestCodec, 512, NativeDurabilityLimits::default(),
                limits).wait(), Err(FrankenError::TooBig)));
        }
    }

    #[test]
    fn native_replay_preserves_out_of_order_writer_ids_and_next_token() {
        let cx = Cx::new(); let vfs = MemoryVfs::new(); let mut db = store(&vfs, &cx);
        let mut earlier = db.begin(&cx).unwrap(); let mut later = db.begin(&cx).unwrap();
        db.write_page(&cx, &mut earlier, page(2), Some(&[1; 512])).unwrap();
        db.write_page(&cx, &mut later, page(3), Some(&[2; 512])).unwrap();
        db.commit(&cx, &mut later, 1).expect("later-started writer commits first");
        db.commit(&cx, &mut earlier, 2).expect("disjoint earlier writer remains valid");
        db.close(&cx).unwrap();
        let limits = NativePageLimits {
            max_retained_page_bytes: 1024, max_versions: 2, ..NativePageLimits::default()
        };
        let (mut recovered, _) = Store::recover(&cx, file(&vfs, &cx, "objects"),
            file(&vfs, &cx, "markers"), TestCodec, 512, NativeDurabilityLimits::default(),
            limits).expect("token order is not commit order");
        let mut view = recovered.begin(&cx).unwrap();
        assert_eq!(view.token().id.get(), 3);
        assert_eq!(recovered.read_page(&cx, &mut view, page(2)).unwrap().unwrap().as_ref(), &[1; 512]);
        assert_eq!(recovered.read_page(&cx, &mut view, page(3)).unwrap().unwrap().as_ref(), &[2; 512]);
        recovered.rollback(&mut view).unwrap(); recovered.close(&cx).unwrap();
    }
}
