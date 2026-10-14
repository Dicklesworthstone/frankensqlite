//! Retained page-history replay after completed native storage recovery.
//!
//! This owns the sealed storage handoff through every borrowed replay attempt.
//! Neither caller-supplied page maps nor a weaker validation callback can replace
//! the stored capsules, read observations, tombstones or exact transaction tokens.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_vfs::VfsFile;

use super::{NativePageLimits, NativePageStore, PageHistory, SharedCodec, corrupt};
use crate::native_commit::durable::{NativeCoordinatorRecovery, NativeObjectCodec};
use crate::native_durability::{NativeDurabilityRecovery, NativeRecovery, RecoveredNativeLog};

impl<S: VfsFile, M: VfsFile, C: NativeObjectCodec> NativePageStore<S, M, C> {
    /// Transfer a quiescent live store into caller-owned writeback recovery.
    ///
    /// Finish or drop every transaction first, including unread snapshots and
    /// indeterminate handles. This consumes the old admission domain, not the
    /// external namespace/append lease, which the caller must retain throughout.
    /// No open, close, read, write, sync, rollback or retry occurs here.
    ///
    /// Both original streams, their acknowledged prefix and any outstanding
    /// source-owned write completion move into `NativeRecovery`. A pending
    /// write may transfer, but still prevents recovery I/O until it settles.
    /// An indeterminate commit is resolved by actual marker/capsule replay;
    /// discarding its old handle does not assert that the commit was aborted.
    /// The returned Arc retains the exact codec, without copying secret keys.
    /// After `prepare`/`finish`, use `NativePageRecovery::with_shared_codec`
    /// with this store's page size and limits to rebuild usable transactions.
    ///
    /// # Errors
    /// Returns the store while a transaction pin remains, close has begun, or
    /// the coordinator still owns an undelivered acknowledgement. No handles
    /// or queued obligations are discarded on refusal.
    #[allow(clippy::result_large_err)] // Refusal must retain the live storage owner.
    pub fn try_into_recovery(
        self,
    ) -> std::result::Result<(NativeRecovery<S, M>, Arc<C>), Self> {
        if self.closed || self.active.iter().any(|lease| lease.strong_count() != 0) {
            return Err(self);
        }
        let Self {
            mut driver, codec, history, owner, active, next_txn_id,
            page_high_water, recovery_required, closed,
        } = self;
        // Page admission precedes staging. Cancellation after a completed
        // stage can leave the lower log healthy while this page attempt is
        // indeterminate. Propagate that existing obligation, exactly as close
        // does, instead of classifying its reservations as healthy queued work.
        if recovery_required {
            driver.abandon_batch();
        }
        match driver.try_into_recovery() {
            Ok((recovery, _shared_codec)) => Ok((recovery, codec)),
            Err(driver) => Err(Self {
                driver, codec, history, owner, active, next_txn_id,
                page_high_water, recovery_required, closed,
            }),
        }
    }
}

struct PreparedPages {
    history: PageHistory,
    next_txn_id: u64,
    page_high_water: u64,
}

/// Rebuild a usable native page store while retaining the original file handles.
///
/// Storage rewrite/sync must already have completed through `NativeRecovery`.
/// This read-only stage independently replays every bound page capsule and its
/// snapshot observations in commit order. It uses the same page admission and
/// retention accounting as ordinary page-store recovery, with no live readers
/// during replay. Superseded page images are discarded, but tombstone stamps and
/// committed tokens remain authoritative for validation and allocation.
///
/// Keep the external namespace/append lease continuously held. Dropping a
/// `prepare` future discards only that attempt's partial page history; this owner
/// retains the streams and codec for retry or explicit close. This does not
/// switch public Connection storage modes or acknowledge an old transaction.
#[must_use]
pub struct NativePageRecovery<S: VfsFile, M: VfsFile, C: NativeObjectCodec> {
    replay: NativeCoordinatorRecovery<S, M, SharedCodec<C>>,
    codec: Arc<C>,
    page_size: u32,
    limits: NativePageLimits,
    prepared: Option<PreparedPages>,
}

impl<S: VfsFile, M: VfsFile, C: NativeObjectCodec> fmt::Debug for NativePageRecovery<S, M, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativePageRecovery")
            .field("page_size", &self.page_size)
            .field("prepared", &self.prepared.is_some())
            .field("replay", &self.replay)
            .finish_non_exhaustive()
    }
}

impl<S: VfsFile, M: VfsFile, C: NativeObjectCodec> NativePageRecovery<S, M, C> {
    /// Retain a sealed storage handoff without I/O or losing it on bad options.
    /// Page size and limits are checked by `prepare` before its first read.
    pub fn new(
        recovered: RecoveredNativeLog<S, M>,
        codec: C,
        page_size: u32,
        limits: NativePageLimits,
    ) -> Self {
        Self::with_shared_codec(recovered, Arc::new(codec), page_size, limits)
    }

    /// Retain an existing shared codec, including one returned by
    /// `into_recovery`, without requiring codec or secret-key duplication.
    pub fn with_shared_codec(
        recovered: RecoveredNativeLog<S, M>,
        codec: Arc<C>,
        page_size: u32,
        limits: NativePageLimits,
    ) -> Self {
        let replay = NativeCoordinatorRecovery::new(
            recovered,
            SharedCodec(Arc::clone(&codec)),
            limits.max_active_transactions.min(NativePageStore::<S, M, C>::MAX_COMMIT_GROUP),
        );
        Self { replay, codec, page_size, limits, prepared: None }
    }

    /// Verified storage prefix and retained tail accounting, not a new commit.
    #[must_use]
    pub const fn report(&self) -> &NativeDurabilityRecovery {
        self.replay.report()
    }

    /// Rebuild page state with mandatory capsule and read-version validation.
    ///
    /// Retries always start from genesis. Errors, cancellation, unwinding and
    /// dropped reads cannot expose a partially reconstructed store. Even after
    /// an earlier successful prepare, a later failed attempt invalidates finish.
    /// No writes, syncs, reopens or transaction-wide locks are introduced here.
    ///
    /// # Errors
    /// Rejects wrong page sizes, invalid/insufficient retention limits, corrupt
    /// proof/capsule bindings, duplicate tokens, stale committed observations,
    /// I/O errors, cancellation and ownership whose close has begun.
    pub async fn prepare(&mut self, cx: &Cx) -> Result<()> {
        self.prepared = None;
        cx.checkpoint().map_err(|_| FrankenError::Interrupt)?;
        let mut history = PageHistory::new(self.page_size, self.limits)?;
        self.replay
            .prepare(cx, |candidate, _| history.replay_candidate(candidate))
            .await?;
        // The coordinator already matched this complete report to its log.
        let tip = self.replay.report().markers.last().map_or(
            fsqlite_types::CommitSeq::ZERO, |marker| marker.commit_seq,
        );
        if history.tip != tip {
            return Err(corrupt("native page replay/driver tip mismatch"));
        }
        let next_txn_id = history.max_txn_id.checked_add(1).ok_or(FrankenError::DatabaseFull)?;
        // Latest tombstones still own their addresses. Never recycle them or
        // enumerate sparse holes, and never use uncommitted allocator metadata.
        let page_high_water = history.pages.last_key_value()
            .map_or(1, |(page, _)| u64::from(page.get()).max(1));
        self.prepared = Some(PreparedPages { history, next_txn_id, page_high_water });
        Ok(())
    }

    /// Install complete page and coordinator state together, without new I/O.
    /// An unprepared/closed owner is returned intact on refusal. Recovered torn
    /// tails remain admission-blocking even after successful semantic replay.
    ///
    /// # Errors
    /// Returns `self` until the most recent prepare succeeds or after close.
    #[allow(clippy::result_large_err)] // Retaining storage ownership is the error contract.
    pub fn finish(mut self) -> std::result::Result<
        (NativePageStore<S, M, C>, NativeDurabilityRecovery), Self,
    > {
        let Some(pages) = self.prepared.take() else { return Err(self); };
        let Self { replay, codec, page_size, limits, .. } = self;
        match replay.finish() {
            Ok((driver, report)) => Ok((NativePageStore {
                driver,
                codec,
                history: pages.history,
                owner: Arc::new(()),
                active: Vec::new(),
                next_txn_id: pages.next_txn_id,
                page_high_water: AtomicU64::new(pages.page_high_water),
                recovery_required: false,
                closed: false,
            }, report)),
            Err(replay) => Err(Self {
                replay, codec, page_size, limits, prepared: Some(pages),
            }),
        }
    }

    /// Return to explicit storage recovery while preserving both handles and
    /// the shared codec. A new storage preparation is required before handoff.
    #[must_use]
    pub fn into_recovery(self) -> (NativeRecovery<S, M>, Arc<C>) {
        let (recovery, _) = self.replay.into_recovery();
        (recovery, self.codec)
    }

    /// Close only the retained streams. Failed close obligations remain owned.
    ///
    /// # Errors
    /// Propagates the VFS close failure; the owner can retry close but not replay.
    pub fn close(&mut self, cx: &Cx) -> Result<()> {
        self.prepared = None;
        self.replay.close(cx)
    }
}

#[cfg(test)]
#[path = "store_recovery_live_tests.rs"]
mod live_transfer_tests;
