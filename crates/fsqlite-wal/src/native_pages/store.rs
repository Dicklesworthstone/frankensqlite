//! Concrete snapshot page transactions over the native durable driver.
//!
//! Open transactions own private overlays; they do not hold a store borrow or
//! a file lock. Only publication requires exclusive access to this owner. This
//! is an in-process page API, not the sealed SQL pager or cross-process MVCC.
//! The caller retains the native log's namespace/append lease, including while
//! an abandoned tracked write settles. The store never creates or deletes files.

use std::collections::{BTreeMap, HashSet};
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
/// All historical versions are retained in this first page-store profile.
/// Reaching a bound refuses new writes before I/O; it never evicts an old
/// snapshot's version. Compaction/GC is a separate integration requirement.
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

/// An owner-bound snapshot and private overlay. Dropping an active transaction
/// discards only this private overlay and releases its active-session slot.
/// Already returned page Arcs may outlive it and are caller-owned memory.
pub struct NativePageTransaction {
    owner: Arc<()>,
    lease: Option<Arc<()>>,
    token: TxnToken,
    snapshot: CommitSeq,
    reads: BTreeMap<PageNumber, CommitSeq>,
    writes: BTreeMap<PageNumber, Option<Arc<[u8]>>>,
    payload_bytes: usize,
    state: NativePageTransactionState,
}
impl NativePageTransaction {
    #[must_use]
    pub const fn snapshot(&self) -> CommitSeq { self.snapshot }
    #[must_use]
    pub const fn token(&self) -> TxnToken { self.token }
    #[must_use]
    pub const fn state(&self) -> NativePageTransactionState { self.state }

    fn finish(&mut self, state: NativePageTransactionState) {
        self.state = state;
        self.lease = None;
        self.reads.clear();
        self.writes.clear();
        self.payload_bytes = 0;
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
}
struct PreparedApply {
    seq: CommitSeq,
    token: TxnToken,
    versions: Vec<(PageNumber, Version)>,
    payload_bytes: usize,
    version_count: usize,
}
impl PageHistory {
    fn new(page_size: u32, limits: NativePageLimits) -> Result<Self> {
        validate_page_size(page_size)?;
        limits.validate()?;
        Ok(Self {
            page_size, limits, tip: CommitSeq::ZERO, pages: BTreeMap::new(),
            tokens: HashSet::new(), max_txn_id: 0, payload_bytes: 0, version_count: 0,
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
    fn prepare(&mut self, capsule: &NativePageCapsule, seq: CommitSeq, token: TxnToken) -> Result<PreparedApply> {
        self.validate(capsule)?;
        if self.tip.get().checked_add(1) != Some(seq.get()) || self.tokens.contains(&token) {
            return Err(corrupt("noncontiguous or duplicate native page commit"));
        }
        let version_count = self.version_count.checked_add(capsule.writes.len())
            .filter(|count| *count <= self.limits.max_versions).ok_or(FrankenError::TooBig)?;
        let payload_bytes = capsule.writes.iter().try_fold(self.payload_bytes, |total, write| {
            total.checked_add(write.data.as_ref().map_or(0, |data| data.len()))
                .filter(|n| *n <= self.limits.max_retained_page_bytes).ok_or(FrankenError::TooBig)
        })?;
        let mut versions = Vec::new();
        versions.try_reserve_exact(capsule.writes.len()).map_err(|_| FrankenError::OutOfMemory)?;
        self.tokens.try_reserve(1).map_err(|_| FrankenError::OutOfMemory)?;
        for write in &capsule.writes {
            // Reserve all publication storage before any irreversible write.
            // Empty precreated entries have no visible page version.
            if self.pages.entry(write.page).or_default().try_reserve(1).is_err() {
                self.pages.retain(|_, versions| !versions.is_empty());
                return Err(FrankenError::OutOfMemory);
            }
            versions.push((write.page, Version { seq, data: write.data.clone() }));
        }
        Ok(PreparedApply { seq, token, versions, payload_bytes, version_count })
    }
    fn apply(&mut self, prepared: PreparedApply) {
        for (page, version) in prepared.versions {
            self.pages.get_mut(&page).expect("page publication storage was reserved").push(version);
        }
        self.tokens.insert(prepared.token);
        self.max_txn_id = self.max_txn_id.max(prepared.token.id.get());
        self.tip = prepared.seq;
        self.payload_bytes = prepared.payload_bytes;
        self.version_count = prepared.version_count;
    }
}

/// Native full-page snapshot transactions, backed by the existing durable
/// coordinator, object codec, and two-stream recovery path. Its callbacks are
/// fixed by this implementation: callers cannot omit page conflict validation.
///
/// Page writes are buffered independently in transaction handles. Commit-order
/// read validation is conservative (not SSI). Public SQL dispatch, native root
/// bootstrap, page allocation, and B-tree predicate tracking remain separate.
/// File creation/namespace durability and the append lease are caller-owned.
pub struct NativePageStore<S: VfsFile, M: VfsFile, C: NativeObjectCodec> {
    driver: DurableWriteCoordinator<S, M, SharedCodec<C>>,
    codec: Arc<C>,
    history: PageHistory,
    owner: Arc<()>,
    active: Vec<Weak<()>>,
    next_txn_id: u64,
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
            recovery_required: false, closed: false,
        })
    }

    /// Replay all bound capsules into versioned pages, validating their physical
    /// read histories in commit order. Orphan objects are not replayed. A corrupt
    /// or unserializable prefix produces no usable store. No supplied metadata
    /// or validation callback can replace the stored page images/read versions.
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
                    candidate.proof.submission.txn_token).map_err(|error| match error {
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
        Ok((Self {
            driver, codec, history, owner: Arc::new(()), active: Vec::new(), next_txn_id,
            recovery_required: false, closed: false,
        }, report))
    }

    #[must_use]
    pub const fn committed_tip(&self) -> CommitSeq { self.history.tip }
    #[must_use]
    pub fn needs_recovery(&self) -> bool { self.recovery_required || self.driver.needs_recovery() }
    #[must_use]
    pub fn outstanding_write(&self) -> Option<VfsWriteCompletion> { self.driver.outstanding_write() }

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
        let lease = Arc::new(());
        self.active.push(Arc::downgrade(&lease));
        self.next_txn_id += 1; // TxnId's domain is narrower than u64.
        Ok(NativePageTransaction {
            owner: Arc::clone(&self.owner), lease: Some(lease),
            token: TxnToken::new(id, TxnEpoch::new(1)), snapshot: self.history.tip,
            reads: BTreeMap::new(), writes: BTreeMap::new(), payload_bytes: 0,
            state: NativePageTransactionState::Active,
        })
    }

    fn observe(&self, txn: &mut NativePageTransaction, page: PageNumber) -> Result<()> {
        if !txn.reads.contains_key(&page) {
            overlay_size(txn.reads.len() + 1, txn.writes.len(), txn.payload_bytes)?;
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
        let old = txn.writes.get(&page).and_then(|value| value.as_ref()).map_or(0, |value| value.len());
        let payload_bytes = txn.payload_bytes - old + data.map_or(0, |bytes| bytes.len());
        overlay_size(txn.reads.len() + usize::from(!txn.reads.contains_key(&page)),
            txn.writes.len() + usize::from(!txn.writes.contains_key(&page)), payload_bytes)?;
        self.observe(txn, page)?;
        txn.writes.insert(page, data.map(Arc::from));
        txn.payload_bytes = payload_bytes;
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
        let bytes = capsule.to_bytes()?;
        let records = self.codec.encode(cx, &bytes)?;
        let object_id = records.first().ok_or_else(|| corrupt("native page encoder returned no symbols"))?.object_id;
        if self.codec.decode(cx, object_id, &records)? != bytes {
            return Err(corrupt("native page codec changed canonical capsule bytes"));
        }
        let seq = self.history.tip.get().checked_add(1).map(CommitSeq::new).ok_or(FrankenError::DatabaseFull)?;
        let prepared = self.history.prepare(&capsule, seq, txn.token)?;
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
