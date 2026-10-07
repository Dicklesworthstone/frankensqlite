//! Transaction-aware, incremental committed images for memory pagers (GH494).
//!
//! Unlike a page-directory primitive, this owner obtains the exact pending
//! commit surface from the real pager, performs the real commit, and reads
//! final page images through a newly pinned transaction. It never publishes
//! pre-commit page-one/freelist bytes, or labels a peer's image as our commit.
//!
//! The first BEGIN (and a BEGIN after an unobserved external commit) seeds one
//! complete image. Otherwise BEGIN shares a root, publication reads only the
//! pending surface, page one, and newly allocated addresses, and snapshot point
//! reads walk at most eight directory levels. Native/file-backed and WAL
//! protocols are deliberately not intercepted by this memory-only owner.
//!
//! The transaction implements the sealed `TransactionHandle` used by storage
//! cursors. Even generic commit/rollback calls stay inside the history owner;
//! no mutable reference to its underlying transaction can escape. This API
//! does not by itself change Connection's SQL COMMIT path. That caller must
//! adopt the owner and use these images instead of its flat page vector.

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::{CommitSeq, PageData, PageNumber, PageSize};
use fsqlite_vfs::MemoryVfs;

use crate::pager::{SimplePager, SimpleTransaction};
use crate::persistent_page_map::PersistentPageMap;
use crate::traits::{JournalMode, MvccPager, PagerCommitState, TransactionHandle, TransactionMode};

/// Counts calls made by this capture, not physical I/O or SQL execution time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PageImageCaptureStats {
    /// A baseline seed, as opposed to a delta from a matched predecessor.
    pub full_capture: bool,
    /// Successful `get_page` calls made by this capture.
    pub pages_read: usize,
}

/// Immutable page image at one pager commit sequence.
///
/// Cloning shares the directory and payloads. Dropping a snapshot may reclaim
/// its uniquely owned changed state; snapshots never form a delta replay chain.
#[derive(Clone, Debug)]
pub struct MemoryPageImage {
    sequence: CommitSeq,
    page_size: PageSize,
    db_size: u32,
    pages: PersistentPageMap<PageData>,
    capture: PageImageCaptureStats,
}

impl MemoryPageImage {
    pub fn sequence(&self) -> CommitSeq {
        self.sequence
    }

    pub fn page_size(&self) -> PageSize {
        self.page_size
    }

    pub fn db_size(&self) -> u32 {
        self.db_size
    }

    pub fn capture_stats(&self) -> PageImageCaptureStats {
        self.capture
    }

    /// A bounded lookup; no current pager read and no history materialization.
    pub fn get_page(&self, page: PageNumber) -> Option<&PageData> {
        self.pages.get(page.get())
    }

    /// Explicit whole-image traversal for export/validation, never publication.
    pub fn iter(&self) -> impl Iterator<Item = (u32, &PageData)> {
        self.pages.iter()
    }

    fn matches(&self, txn: &SimpleTransaction<MemoryVfs>) -> bool {
        txn.published_visible_commit_seq_hint() == Some(self.sequence)
            && txn.snapshot_db_size() == self.db_size
            && txn.page_size() == self.page_size
    }
}

/// Both variants mean the underlying transaction is terminally committed.
///
/// A failure to capture history must NOT be mistaken for a failed SQL commit,
/// retried as another write, or sent through transaction rollback semantics.
#[derive(Debug)]
#[must_use]
pub enum MemorySnapshotCommit {
    Captured(MemoryPageImage),
    CaptureFailed(FrankenError),
}

/// Reconciliation of the exact previously attempted commit, never a new write.
#[derive(Debug)]
#[must_use]
pub enum MemorySnapshotSettlement {
    NotCommitted,
    Pending(PagerCommitState),
    Committed(MemorySnapshotCommit),
}

/// A serialized snapshot owner bound to one exact in-memory pager.
///
/// The owner supplies transaction handles, so a caller cannot accidentally
/// combine another database's dirty-page list with this database's history.
/// Existing clones of the pager may still publish: exact sequence checks then
/// reject mixed capture and the next BEGIN reseeds from its own pinned view.
/// Only the latest image is retained here; callers own their history policy.
pub struct MemoryPageSnapshots {
    pager: SimplePager<MemoryVfs>,
    current: Option<MemoryPageImage>,
}

impl MemoryPageSnapshots {
    pub fn new(pager: SimplePager<MemoryVfs>) -> Self {
        Self {
            pager,
            current: None,
        }
    }

    /// Last observed image, not a claim that no peer has since committed.
    /// BEGIN binds to a real transaction before deciding whether to reuse it.
    pub fn current(&self) -> Option<&MemoryPageImage> {
        self.current.as_ref()
    }

    /// Read/configure the original pager. Committed pager writes outside this
    /// owner invalidate reuse by their bound sequence on the next BEGIN.
    pub fn pager(&self) -> &SimplePager<MemoryVfs> {
        &self.pager
    }

    pub async fn begin(
        &mut self,
        cx: &Cx,
        mode: TransactionMode,
    ) -> Result<MemorySnapshotTransaction<'_>> {
        if self.pager.journal_mode() != JournalMode::Delete
            || mode == TransactionMode::Concurrent
        {
            return Err(FrankenError::Unsupported);
        }
        let mut inner = self.pager.begin(cx, mode).await?;
        let base = if let Some(image) = self
            .current
            .as_ref()
            .filter(|image| image.matches(&inner))
        {
            let mut image = image.clone();
            image.capture = PageImageCaptureStats::default();
            image
        } else {
            match capture_full(cx, &inner).await {
                Ok(image) => image,
                Err(error) => {
                    // No user's writes exist yet. Preserve the capture error;
                    // the real transaction retains its own cleanup obligations.
                    let _ = inner.rollback(cx).await;
                    return Err(error);
                }
            }
        };
        self.current = Some(base.clone());
        Ok(MemorySnapshotTransaction {
            owner: self,
            inner,
            base,
            attempt: None,
            finished: false,
        })
    }
}

struct CommitAttempt {
    pages: Vec<PageNumber>,
    has_writes: bool,
}

/// Exclusive access to this history owner for one real pager transaction.
///
/// Implements the same sealed `TransactionHandle` as an ordinary pager handle,
/// so generic storage cursors cannot bypass history at finalization. The
/// inherent commit/settlement methods additionally return the capture outcome;
/// the trait methods report physical transaction outcomes and log a failed
/// history capture without turning an already committed write into an error.
/// Dropping this wrapper leaves cleanup to the original pager handle, never an
/// invented rollback protocol. An interrupted commit invalidates image reuse.
pub struct MemorySnapshotTransaction<'a> {
    owner: &'a mut MemoryPageSnapshots,
    inner: SimpleTransaction<MemoryVfs>,
    base: MemoryPageImage,
    attempt: Option<CommitAttempt>,
    finished: bool,
}

impl MemorySnapshotTransaction<'_> {
    pub fn baseline_capture_stats(&self) -> PageImageCaptureStats {
        self.base.capture
    }

    fn ensure_editable(&self) -> Result<()> {
        if self.finished || self.attempt.is_some() {
            return Err(FrankenError::BusyRecovery);
        }
        Ok(())
    }

    /// Borrow the guarded transaction for generic storage code. This is NOT a
    /// raw pager handle: its commit, retain and rollback entry points also go
    /// through the snapshot owner. Each mutator rechecks the guard, including
    /// when a caller retains this borrow across finalization.
    ///
    /// ```compile_fail
    /// use fsqlite_pager::{MemorySnapshotTransaction, SimpleTransaction};
    /// use fsqlite_vfs::MemoryVfs;
    /// fn escape<'a, 'owner>(
    ///     transaction: &'a mut MemorySnapshotTransaction<'owner>,
    /// ) -> &'a mut SimpleTransaction<MemoryVfs> {
    ///     transaction.transaction_mut().unwrap()
    /// }
    /// ```
    pub fn transaction_mut(&mut self) -> Result<&mut Self> {
        self.ensure_editable()?;
        Ok(self)
    }

    pub fn pager_commit_state(&self) -> PagerCommitState {
        self.inner.pager_commit_state()
    }

    /// Commit once, then capture history. An error requires inspecting or
    /// settling this same handle; it is not permission to repeat the write.
    pub async fn commit(&mut self, cx: &Cx) -> Result<MemorySnapshotCommit> {
        self.ensure_editable()?;
        if self.owner.pager.journal_mode() != JournalMode::Delete {
            return Err(FrankenError::Unsupported);
        }
        // Validate before physical commit. A deferred transaction must not
        // merge a stale baseline with a newer unrelated database generation.
        let has_writes = self.inner.has_pending_writes();
        if has_writes
            && self.owner.pager.published_snapshot().visible_commit_seq != self.base.sequence
        {
            return Err(stale_image());
        }
        // NOT write_set_page_numbers(): that omits synthesized freelist trunks.
        let pages = self.inner.pending_commit_pages()?;
        if !has_writes && !pages.is_empty() {
            return Err(FrankenError::internal(
                "read-only snapshot commit has pending pages",
            ));
        }
        self.attempt = Some(CommitAttempt {
            pages,
            has_writes,
        });
        // Do this BEFORE awaiting commit. Dropping a polled future at any
        // subsequent await cannot leave an allegedly current reusable image.
        self.owner.current = None;
        self.inner.commit(cx).await?;
        Ok(self.finish_committed(cx).await)
    }

    pub async fn settle_commit(&mut self, cx: &Cx) -> Result<MemorySnapshotSettlement> {
        if self.finished {
            return Err(FrankenError::BusyRecovery);
        }
        if self.attempt.is_none() {
            return Ok(MemorySnapshotSettlement::NotCommitted);
        }
        match self.inner.settle_commit(cx).await? {
            PagerCommitState::NotCommitted => {
                self.attempt = None;
                self.owner.current = Some(self.base.clone());
                Ok(MemorySnapshotSettlement::NotCommitted)
            }
            PagerCommitState::Committed => {
                Ok(MemorySnapshotSettlement::Committed(
                    self.finish_committed(cx).await,
                ))
            }
            state => Ok(MemorySnapshotSettlement::Pending(state)),
        }
    }

    pub async fn rollback(&mut self, cx: &Cx) -> Result<()> {
        if self.finished || self.inner.pager_commit_state().retains_commit_obligation() {
            return Err(FrankenError::BusyRecovery);
        }
        self.inner.rollback(cx).await?;
        self.finished = true;
        self.attempt = None;
        self.owner.current = Some(self.base.clone());
        Ok(())
    }

    async fn finish_committed(&mut self, cx: &Cx) -> MemorySnapshotCommit {
        let Some(attempt) = self.attempt.as_ref() else {
            self.finished = true;
            return MemorySnapshotCommit::CaptureFailed(FrankenError::internal(
                "missing snapshot commit attempt",
            ));
        };
        if !attempt.has_writes {
            // The no-write transaction's own view, not a later peer's image.
            let mut image = self.base.clone();
            image.capture = PageImageCaptureStats::default();
            self.owner.current = Some(image.clone());
            self.attempt = None;
            self.finished = true;
            return MemorySnapshotCommit::Captured(image);
        }
        // Retain the original receipt until capture has actually completed.
        // Dropping this future at either reader await must still permit
        // settle_commit() to report the successful physical commit and retry
        // history capture, rather than lose the receipt in a local variable.
        let pages = attempt.pages.clone();
        let mut reader = match self
            .owner
            .pager
            .begin(cx, TransactionMode::ReadOnly)
            .await
        {
            Ok(reader) => reader,
            Err(error) => {
                self.attempt = None;
                self.finished = true;
                return MemorySnapshotCommit::CaptureFailed(error);
            }
        };
        let capture = capture_changed(cx, &reader, &self.base, pages).await;
        let released = reader.rollback(cx).await;
        self.attempt = None;
        self.finished = true;
        match capture.and_then(|image| released.map(|()| image)) {
            Ok(image) => {
                self.owner.current = Some(image.clone());
                MemorySnapshotCommit::Captured(image)
            }
            Err(error) => MemorySnapshotCommit::CaptureFailed(error),
        }
    }
}

impl crate::traits::sealed::Sealed for MemorySnapshotTransaction<'_> {}

impl TransactionHandle for MemorySnapshotTransaction<'_> {
    async fn get_page<'a>(&'a self, cx: &'a Cx, page: PageNumber) -> Result<PageData> {
        self.ensure_editable()?;
        self.inner.get_page(cx, page).await
    }

    fn prefetch_page_hint(&self, cx: &Cx, page: PageNumber) {
        if self.ensure_editable().is_ok() {
            self.inner.prefetch_page_hint(cx, page);
        }
    }

    fn forget_cached_page(&self, page: PageNumber) {
        if self.ensure_editable().is_ok() {
            self.inner.forget_cached_page(page);
        }
    }

    async fn write_page<'a>(
        &'a mut self,
        cx: &'a Cx,
        page: PageNumber,
        data: &'a [u8],
    ) -> Result<()> {
        self.ensure_editable()?;
        self.inner.write_page(cx, page, data).await
    }

    async fn write_page_data<'a>(
        &'a mut self,
        cx: &'a Cx,
        page: PageNumber,
        data: PageData,
    ) -> Result<()> {
        self.ensure_editable()?;
        self.inner.write_page_data(cx, page, data).await
    }

    fn try_take_staged_page_data(&mut self, page: PageNumber) -> Option<PageData> {
        self.ensure_editable().ok()?;
        self.inner.try_take_staged_page_data(page)
    }

    fn try_mutate_staged_page_data(
        &mut self,
        page: PageNumber,
        mutate: &mut dyn FnMut(&mut PageData),
    ) -> bool {
        self.ensure_editable().is_ok()
            && self.inner.try_mutate_staged_page_data(page, mutate)
    }

    async fn restore_staged_page_data<'a>(
        &'a mut self,
        cx: &'a Cx,
        page: PageNumber,
        data: PageData,
    ) -> Result<()> {
        self.ensure_editable()?;
        self.inner.restore_staged_page_data(cx, page, data).await
    }

    async fn allocate_page<'a>(&'a mut self, cx: &'a Cx) -> Result<PageNumber> {
        self.ensure_editable()?;
        self.inner.allocate_page(cx).await
    }

    async fn free_page<'a>(&'a mut self, cx: &'a Cx, page: PageNumber) -> Result<()> {
        self.ensure_editable()?;
        self.inner.free_page(cx, page).await
    }

    async fn commit<'a>(&'a mut self, cx: &'a Cx) -> Result<()> {
        report_trait_capture(MemorySnapshotTransaction::commit(self, cx).await?);
        Ok(())
    }

    fn pager_commit_state(&self) -> PagerCommitState {
        self.inner.pager_commit_state()
    }

    async fn settle_commit<'a>(&'a mut self, cx: &'a Cx) -> Result<PagerCommitState> {
        if self.finished {
            return Ok(self.inner.pager_commit_state());
        }
        match MemorySnapshotTransaction::settle_commit(self, cx).await? {
            MemorySnapshotSettlement::NotCommitted => Ok(PagerCommitState::NotCommitted),
            MemorySnapshotSettlement::Pending(state) => Ok(state),
            MemorySnapshotSettlement::Committed(outcome) => {
                report_trait_capture(outcome);
                Ok(PagerCommitState::Committed)
            }
        }
    }

    async fn commit_and_retain<'a>(&'a mut self, cx: &'a Cx) -> Result<bool> {
        // The retained memory overlay is not necessarily readable through a
        // separate pager reader. Use the trait's documented non-retaining
        // fallback until that protocol can supply an exact captured image.
        // Never delegate directly and silently lose a committed generation.
        TransactionHandle::commit(self, cx).await?;
        Ok(false)
    }

    fn is_writer(&self) -> bool {
        self.inner.is_writer()
    }

    fn has_pending_writes(&self) -> bool {
        self.inner.has_pending_writes()
    }

    fn published_visible_commit_seq_hint(&self) -> Option<CommitSeq> {
        self.inner.published_visible_commit_seq_hint()
    }

    fn pending_commit_pages(&self) -> Result<Vec<PageNumber>> {
        self.ensure_editable()?;
        self.inner.pending_commit_pages()
    }

    fn pending_conflict_pages(&self) -> Result<Vec<PageNumber>> {
        self.ensure_editable()?;
        self.inner.pending_conflict_pages()
    }

    fn pending_conflict_pages_conservative(&self) -> Vec<PageNumber> {
        self.inner.pending_conflict_pages_conservative()
    }

    fn write_set_page_numbers(&self) -> Vec<PageNumber> {
        self.inner.write_set_page_numbers()
    }

    fn page_one_in_pending_commit_surface(&self) -> Result<bool> {
        self.ensure_editable()?;
        self.inner.page_one_in_pending_commit_surface()
    }

    fn page_size(&self) -> PageSize {
        self.inner.page_size()
    }

    fn allocate_page_requires_page_one_conflict_tracking(&self) -> Result<bool> {
        self.ensure_editable()?;
        self.inner.allocate_page_requires_page_one_conflict_tracking()
    }

    fn free_page_requires_page_one_conflict_tracking(&self, page: PageNumber) -> Result<bool> {
        self.ensure_editable()?;
        self.inner.free_page_requires_page_one_conflict_tracking(page)
    }

    fn write_page_requires_page_one_conflict_tracking(&self, page: PageNumber) -> Result<bool> {
        self.ensure_editable()?;
        self.inner.write_page_requires_page_one_conflict_tracking(page)
    }

    async fn rollback<'a>(&'a mut self, cx: &'a Cx) -> Result<()> {
        MemorySnapshotTransaction::rollback(self, cx).await
    }

    fn record_write_witness(&mut self, cx: &Cx, key: fsqlite_types::WitnessKey) {
        if self.ensure_editable().is_ok() {
            self.inner.record_write_witness(cx, key);
        }
    }

    fn savepoint(&mut self, cx: &Cx, name: &str) -> Result<()> {
        self.ensure_editable()?;
        self.inner.savepoint(cx, name)
    }

    fn release_savepoint(&mut self, cx: &Cx, name: &str) -> Result<()> {
        self.ensure_editable()?;
        self.inner.release_savepoint(cx, name)
    }

    fn rollback_to_savepoint(&mut self, cx: &Cx, name: &str) -> Result<()> {
        self.ensure_editable()?;
        self.inner.rollback_to_savepoint(cx, name)
    }
}

fn report_trait_capture(outcome: MemorySnapshotCommit) {
    if let MemorySnapshotCommit::CaptureFailed(error) = outcome {
        // Trait callers may retry Err as a new transaction. Capture is optional
        // history AFTER a successful physical commit, not permission to retry.
        tracing::warn!(
            target: "fsqlite.memory_snapshot",
            "memory transaction committed but history capture failed: {error}"
        );
    }
}

fn stale_image() -> FrankenError {
    FrankenError::BusySnapshot {
        conflicting_pages: "memory history does not match the pinned commit sequence".to_owned(),
    }
}

fn bound_sequence(txn: &SimpleTransaction<MemoryVfs>) -> Result<CommitSeq> {
    txn.published_visible_commit_seq_hint()
        .ok_or_else(stale_image)
}

async fn read_image_page(
    cx: &Cx,
    txn: &SimpleTransaction<MemoryVfs>,
    page: PageNumber,
) -> Result<PageData> {
    let data = txn.get_page(cx, page).await?;
    if data.len() != txn.page_size().as_usize() {
        return Err(FrankenError::internal("memory snapshot page geometry changed"));
    }
    Ok(data)
}

async fn capture_full(cx: &Cx, txn: &SimpleTransaction<MemoryVfs>) -> Result<MemoryPageImage> {
    let sequence = bound_sequence(txn)?;
    let db_size = txn.snapshot_db_size();
    let mut pages = PersistentPageMap::new();
    for number in 1..=db_size {
        let page = PageNumber::new(number)
            .ok_or_else(|| FrankenError::internal("invalid snapshot page number"))?;
        pages.insert(number, read_image_page(cx, txn, page).await?);
    }
    Ok(MemoryPageImage {
        sequence,
        page_size: txn.page_size(),
        db_size,
        pages,
        capture: PageImageCaptureStats {
            full_capture: true,
            pages_read: db_size as usize,
        },
    })
}

async fn capture_changed(
    cx: &Cx,
    txn: &SimpleTransaction<MemoryVfs>,
    base: &MemoryPageImage,
    mut changed: Vec<PageNumber>,
) -> Result<MemoryPageImage> {
    let sequence = bound_sequence(txn)?;
    // One single-writer memory commit advances this pager's sequence once.
    // A peer committed before we pinned this reader? Reject; do not hide the
    // race with a full scan and label the peer's bytes with our commit number.
    if base.sequence.get().checked_add(1) != Some(sequence.get())
        || base.page_size != txn.page_size()
    {
        return Err(stale_image());
    }
    let db_size = txn.snapshot_db_size();
    let mut pages = base.pages.clone();
    pages.truncate(db_size);
    if db_size > 0 {
        changed.push(PageNumber::ONE);
    }
    // Allocation is touched state too. Include new zero-filled address gaps;
    // never infer them from arbitrary stale bytes in the previous image.
    if db_size > base.db_size {
        for number in base.db_size + 1..=db_size {
            changed.push(
                PageNumber::new(number)
                    .ok_or_else(|| FrankenError::internal("invalid snapshot allocation"))?,
            );
        }
    }
    changed.sort_unstable();
    changed.dedup();
    let mut pages_read = 0;
    for page in changed {
        if page.get() <= db_size {
            pages.insert(page.get(), read_image_page(cx, txn, page).await?);
            pages_read += 1;
        }
    }
    if pages.len() != db_size as usize {
        return Err(FrankenError::internal("incomplete committed memory page image"));
    }
    Ok(MemoryPageImage {
        sequence,
        page_size: base.page_size,
        db_size,
        pages,
        capture: PageImageCaptureStats {
            full_capture: false,
            pages_read,
        },
    })
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use fsqlite_vfs::MemoryVfsConfig;

    async fn owner() -> MemoryPageSnapshots {
        let pager = SimplePager::open(MemoryVfs::new(), Path::new("/history.db"), PageSize::DEFAULT)
            .await.expect("open actual memory pager");
        MemoryPageSnapshots::new(pager)
    }

    fn captured(result: MemorySnapshotCommit) -> MemoryPageImage {
        match result {
            MemorySnapshotCommit::Captured(image) => image,
            MemorySnapshotCommit::CaptureFailed(error) => panic!("capture after commit: {error}"),
        }
    }

    async fn append(txn: &mut MemorySnapshotTransaction<'_>, tag: u8) -> PageNumber {
        let inner = txn.transaction_mut().expect("editable transaction");
        let page = inner.allocate_page(&Cx::new()).await.expect("allocate");
        let bytes = vec![tag; inner.page_size().as_usize()];
        inner.write_page(&Cx::new(), page, &bytes).await.expect("write");
        page
    }

    async fn overwrite(txn: &mut MemorySnapshotTransaction<'_>, page: PageNumber, tag: u8) {
        let inner = txn.transaction_mut().expect("editable transaction");
        let bytes = vec![tag; inner.page_size().as_usize()];
        inner.write_page(&Cx::new(), page, &bytes).await.expect("overwrite");
    }

    // Deliberately generic: these are the TransactionHandle entry points a
    // storage cursor calls, not the wrapper's differently typed inherent API.
    async fn generic_write<T: TransactionHandle>(txn: &mut T, cx: &Cx, tag: u8) -> PageNumber {
        let page = txn.allocate_page(cx).await.unwrap();
        let mut data = PageData::zeroed(txn.page_size());
        data.as_bytes_mut().fill(tag);
        txn.write_page_data(cx, page, data).await.unwrap();
        assert!(txn.has_pending_writes());
        assert!(txn.pending_commit_pages().unwrap().contains(&page));
        assert_eq!(txn.get_page(cx, page).await.unwrap().as_bytes()[0], tag);
        page
    }

    async fn generic_commit<T: TransactionHandle>(txn: &mut T, cx: &Cx) {
        txn.commit(cx).await.unwrap();
    }

    async fn generic_rollback<T: TransactionHandle>(txn: &mut T, cx: &Cx) {
        txn.rollback(cx).await.unwrap();
    }

    async fn assert_frozen<T: TransactionHandle>(txn: &mut T, cx: &Cx, page: PageNumber) {
        let page_size = txn.page_size();
        let bytes = vec![0xff; page_size.as_usize()];
        assert!(matches!(txn.write_page(cx, page, &bytes).await, Err(FrankenError::BusyRecovery)));
        assert!(matches!(txn.write_page_data(cx, page, PageData::zeroed(page_size)).await,
            Err(FrankenError::BusyRecovery)));
        assert!(matches!(txn.restore_staged_page_data(cx, page, PageData::zeroed(page_size)).await,
            Err(FrankenError::BusyRecovery)));
        assert!(matches!(txn.allocate_page(cx).await, Err(FrankenError::BusyRecovery)));
        assert!(matches!(txn.free_page(cx, page).await, Err(FrankenError::BusyRecovery)));
        assert!(matches!(txn.savepoint(cx, "blocked"), Err(FrankenError::BusyRecovery)));
        assert!(matches!(txn.release_savepoint(cx, "blocked"), Err(FrankenError::BusyRecovery)));
        assert!(matches!(txn.rollback_to_savepoint(cx, "blocked"), Err(FrankenError::BusyRecovery)));
        assert!(matches!(txn.pending_commit_pages(), Err(FrankenError::BusyRecovery)));
        assert!(matches!(txn.get_page(cx, page).await, Err(FrankenError::BusyRecovery)));
        assert!(txn.try_take_staged_page_data(page).is_none());
        let mut called = false;
        assert!(!txn.try_mutate_staged_page_data(page, &mut |_| called = true));
        assert!(!called, "a frozen handle must not invoke a mutation callback");
    }

    #[test]
    fn generic_commit_through_the_handed_out_handle_cannot_bypass_history() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut owner = owner().await;
            let (page, previous_sequence) = {
                let mut txn = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                let previous_sequence = txn.base.sequence();
                let handle = txn.transaction_mut().unwrap();
                let page = generic_write(handle, &cx, 0x35).await;
                generic_commit(handle, &cx).await;
                // The borrow obtained before COMMIT is still live here. Merely
                // guarding the accessor, rather than each operation, is unsafe.
                assert_frozen(handle, &cx, page).await;
                assert!(txn.transaction_mut().is_err());
                (page, previous_sequence)
            };
            let image = owner.current().expect("generic commit must publish history");
            assert_eq!(image.sequence().get(), previous_sequence.get() + 1);
            assert_eq!(image.get_page(page).unwrap().as_bytes()[0], 0x35);
            assert_live(&owner, image).await;
        });
    }

    #[test]
    fn generic_retain_entry_point_cannot_escape_history_capture() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut owner = owner().await;
            let page = {
                let mut txn = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                let handle = txn.transaction_mut().unwrap();
                let page = generic_write(handle, &cx, 0x46).await;
                assert!(!handle.commit_and_retain(&cx).await.unwrap(),
                    "the snapshot handle uses the trait's final-commit fallback");
                assert_frozen(handle, &cx, page).await;
                page
            };
            let image = owner.current().unwrap().clone();
            assert_eq!(image.get_page(page).unwrap().as_bytes()[0], 0x46);
            assert_live(&owner, &image).await;
            {
                let mut next = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                assert_eq!(next.baseline_capture_stats().pages_read, 0);
                overwrite(&mut next, page, 0x47).await;
                generic_rollback(next.transaction_mut().unwrap(), &cx).await;
            }
            assert_eq!(owner.current().unwrap().sequence(), image.sequence());
            assert_live(&owner, &image).await;
        });
    }

    #[test]
    fn generic_rollback_retires_the_handle_and_keeps_the_committed_image() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut owner = owner().await;
            let (hot, before) = {
                let mut txn = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                let hot = append(&mut txn, 0x57).await;
                (hot, captured(txn.commit(&cx).await.unwrap()))
            };
            {
                let mut txn = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                overwrite(&mut txn, hot, 0xee).await;
                let handle = txn.transaction_mut().unwrap();
                generic_write(handle, &cx, 0xff).await;
                generic_rollback(handle, &cx).await;
                assert_frozen(handle, &cx, hot).await;
                assert!(txn.transaction_mut().is_err());
            }
            assert_eq!(owner.current().unwrap().sequence(), before.sequence());
            assert_eq!(owner.current().unwrap().get_page(hot).unwrap().as_bytes()[0], 0x57);
            assert_live(&owner, &before).await;
        });
    }

    #[test]
    fn generic_settlement_keeps_capture_failure_distinct_from_failed_commit() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut owner = owner().await;
            let hot = {
                let mut txn = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                let hot = append(&mut txn, 0x68).await;
                captured(txn.commit(&cx).await.unwrap());
                hot
            };
            {
                let mut txn = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                overwrite(&mut txn, hot, 0x69).await;
                // A real physical commit interrupted before history capture.
                txn.attempt = Some(CommitAttempt {
                    pages: txn.inner.pending_commit_pages().unwrap(),
                    has_writes: true,
                });
                txn.owner.current = None;
                txn.inner.commit(&cx).await.unwrap();
                // Advancing the live pager makes capture of our old receipt
                // fail. That must not make a generic caller retry its SQL.
                let mut peer = txn.owner.pager.begin(&cx, TransactionMode::Immediate).await.unwrap();
                peer.write_page(&cx, hot, &vec![0x6a; PageSize::DEFAULT.as_usize()]).await.unwrap();
                peer.commit(&cx).await.unwrap();
                drop(peer);
                let visible = txn.owner.pager.published_snapshot().visible_commit_seq;
                assert_eq!(TransactionHandle::settle_commit(&mut txn, &cx).await.unwrap(),
                    PagerCommitState::Committed);
                assert!(txn.owner.current.is_none());
                assert_frozen(&mut txn, &cx, hot).await;
                assert!(TransactionHandle::rollback(&mut txn, &cx).await.is_err());
                assert_eq!(TransactionHandle::settle_commit(&mut txn, &cx).await.unwrap(),
                    PagerCommitState::Committed);
                assert_eq!(txn.owner.pager.published_snapshot().visible_commit_seq, visible,
                    "settlement must not create another commit");
            }
            let image = {
                let mut next = owner.begin(&cx, TransactionMode::ReadOnly).await.unwrap();
                assert!(next.baseline_capture_stats().full_capture);
                captured(next.commit(&cx).await.unwrap())
            };
            assert_eq!(image.get_page(hot).unwrap().as_bytes()[0], 0x6a);
            assert_live(&owner, &image).await;
        });
    }

    #[test]
    fn generic_unpolled_operations_do_not_mutate_or_start_finalization() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut owner = owner().await;
            let image = {
                let mut txn = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                let page = append(&mut txn, 0x7b).await;
                let pending = txn.inner.pending_commit_pages().unwrap();
                let bytes = vec![0xff; PageSize::DEFAULT.as_usize()];
                drop(TransactionHandle::write_page(&mut txn, &cx, page, &bytes));
                drop(TransactionHandle::allocate_page(&mut txn, &cx));
                drop(TransactionHandle::free_page(&mut txn, &cx, page));
                drop(TransactionHandle::commit(&mut txn, &cx));
                drop(TransactionHandle::commit_and_retain(&mut txn, &cx));
                drop(TransactionHandle::rollback(&mut txn, &cx));
                drop(TransactionHandle::settle_commit(&mut txn, &cx));
                assert!(txn.attempt.is_none());
                assert!(!txn.finished);
                assert_eq!(txn.inner.pending_commit_pages().unwrap(), pending);
                assert_eq!(txn.get_page(&cx, page).await.unwrap().as_bytes()[0], 0x7b);
                captured(txn.commit(&cx).await.unwrap())
            };
            assert_live(&owner, &image).await;
        });
    }

    // Independent oracle: scan the complete real pager image, not a second
    // delta algorithm or only the pages named by the publisher under test.
    async fn assert_live(owner: &MemoryPageSnapshots, image: &MemoryPageImage) {
        let cx = Cx::new();
        let mut reader = owner.pager().begin(&cx, TransactionMode::ReadOnly).await.expect("oracle begin");
        assert_eq!(Some(image.sequence()), reader.published_visible_commit_seq_hint());
        assert_eq!(image.db_size(), reader.snapshot_db_size());
        assert_eq!(image.iter().count(), reader.snapshot_db_size() as usize);
        for number in 1..=reader.snapshot_db_size() {
            let page = PageNumber::new(number).unwrap();
            let expected = reader.get_page(&cx, page).await.expect("oracle page");
            assert_eq!(image.get_page(page).unwrap().as_bytes(), expected.as_bytes(), "page {number}");
        }
        reader.rollback(&cx).await.expect("oracle release");
    }

    #[test]
    fn committed_images_share_untouched_pages_and_preserve_history() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut owner = owner().await;
            let (hot, cold, before) = {
                let mut txn = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                let hot = append(&mut txn, 0x31).await;
                let cold = append(&mut txn, 0x42).await;
                (hot, cold, captured(txn.commit(&cx).await.unwrap()))
            };
            assert_live(&owner, &before).await;
            let after = {
                let mut txn = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                assert_eq!(txn.baseline_capture_stats().pages_read, 0);
                overwrite(&mut txn, hot, 0x53).await;
                captured(txn.commit(&cx).await.unwrap())
            };
            assert_eq!(before.get_page(hot).unwrap().as_bytes()[0], 0x31);
            assert_eq!(after.get_page(hot).unwrap().as_bytes()[0], 0x53);
            assert!(std::ptr::eq(before.get_page(cold).unwrap(), after.get_page(cold).unwrap()));
            assert_eq!(after.capture_stats().pages_read, 2); // hot page plus canonical page one
            assert_live(&owner, &after).await;
        });
    }

    #[test]
    fn savepoint_and_full_rollback_never_publish_discarded_writes() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut owner = owner().await;
            let (hot, cold, original) = {
                let mut txn = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                let hot = append(&mut txn, 0x11).await;
                let cold = append(&mut txn, 0x22).await;
                (hot, cold, captured(txn.commit(&cx).await.unwrap()))
            };
            let committed = {
                let mut txn = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                overwrite(&mut txn, hot, 0x33).await;
                txn.transaction_mut().unwrap().savepoint(&cx, "s").unwrap();
                overwrite(&mut txn, hot, 0xee).await;
                append(&mut txn, 0xdd).await;
                txn.transaction_mut().unwrap().free_page(&cx, cold).await.unwrap();
                txn.transaction_mut().unwrap().rollback_to_savepoint(&cx, "s").unwrap();
                txn.transaction_mut().unwrap().release_savepoint(&cx, "s").unwrap();
                captured(txn.commit(&cx).await.unwrap())
            };
            assert_eq!(committed.get_page(hot).unwrap().as_bytes()[0], 0x33);
            assert_eq!(committed.get_page(cold).unwrap().as_bytes()[0], 0x22);
            assert_live(&owner, &committed).await;
            {
                let mut txn = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                overwrite(&mut txn, hot, 0xff).await;
                append(&mut txn, 0xcc).await;
                txn.rollback(&cx).await.unwrap();
            }
            assert_live(&owner, owner.current().unwrap()).await;
            assert_eq!(owner.current().unwrap().sequence(), committed.sequence());
            assert_eq!(owner.current().unwrap().get_page(hot).unwrap().as_bytes()[0], 0x33);
            assert_eq!(original.get_page(hot).unwrap().as_bytes()[0], 0x11);
        });
    }

    #[test]
    fn dropped_writer_and_read_only_completion_keep_the_baseline() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut owner = owner().await;
            let (hot, original) = {
                let mut txn = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                let hot = append(&mut txn, 0x51).await;
                (hot, captured(txn.commit(&cx).await.unwrap()))
            };
            {
                let mut txn = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                overwrite(&mut txn, hot, 0xaa).await;
                // Real SimpleTransaction::drop owns rollback, not the history.
            }
            assert_live(&owner, &original).await;
            let no_write = {
                let mut txn = owner.begin(&cx, TransactionMode::ReadOnly).await.unwrap();
                assert_eq!(txn.baseline_capture_stats().pages_read, 0);
                captured(txn.commit(&cx).await.unwrap())
            };
            assert_eq!(no_write.sequence(), original.sequence());
            assert_eq!(no_write.capture_stats().pages_read, 0);
            assert_live(&owner, &no_write).await;
        });
    }

    #[test]
    fn rollback_to_before_all_writes_does_not_require_a_new_seed() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut owner = owner().await;
            let (hot, before) = {
                let mut txn = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                let hot = append(&mut txn, 0x61).await;
                (hot, captured(txn.commit(&cx).await.unwrap()))
            };
            {
                let mut txn = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                txn.transaction_mut().unwrap().savepoint(&cx, "all").unwrap();
                overwrite(&mut txn, hot, 0xee).await;
                txn.transaction_mut().unwrap().rollback_to_savepoint(&cx, "all").unwrap();
                txn.transaction_mut().unwrap().release_savepoint(&cx, "all").unwrap();
                let image = captured(txn.commit(&cx).await.unwrap());
                assert_eq!(image.sequence(), before.sequence());
                assert_eq!(image.capture_stats().pages_read, 0);
            }
            let mut txn = owner.begin(&cx, TransactionMode::ReadOnly).await.unwrap();
            assert_eq!(txn.baseline_capture_stats().pages_read, 0);
            txn.rollback(&cx).await.unwrap();
        });
    }

    #[test]
    fn freelist_synthesis_and_later_allocations_match_complete_pager_image() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut owner = owner().await;
            let (victim, original) = {
                let mut txn = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                let victim = append(&mut txn, 0x71).await;
                append(&mut txn, 0x72).await;
                append(&mut txn, 0x73).await;
                (victim, captured(txn.commit(&cx).await.unwrap()))
            };
            let freed = {
                let mut txn = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                txn.transaction_mut().unwrap().free_page(&cx, victim).await.unwrap();
                captured(txn.commit(&cx).await.unwrap())
            };
            assert_live(&owner, &freed).await;
            let (allocated, latest) = {
                let mut txn = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                let allocated = append(&mut txn, 0x74).await;
                (allocated, captured(txn.commit(&cx).await.unwrap()))
            };
            assert_eq!(latest.get_page(allocated).unwrap().as_bytes()[0], 0x74);
            assert_eq!(original.get_page(victim).unwrap().as_bytes()[0], 0x71);
            assert_live(&owner, &latest).await;
        });
    }

    #[test]
    fn external_commit_reseeds_from_a_pinned_baseline() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut owner = owner().await;
            let hot = {
                let mut txn = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                let hot = append(&mut txn, 0x81).await;
                captured(txn.commit(&cx).await.unwrap());
                hot
            };
            {
                let mut peer = owner.pager().begin(&cx, TransactionMode::Immediate).await.unwrap();
                peer.write_page(&cx, hot, &vec![0x82; PageSize::DEFAULT.as_usize()]).await.unwrap();
                peer.commit(&cx).await.unwrap();
            }
            let image = {
                let mut txn = owner.begin(&cx, TransactionMode::ReadOnly).await.unwrap();
                assert!(txn.baseline_capture_stats().full_capture);
                assert_eq!(txn.baseline_capture_stats().pages_read, txn.base.db_size as usize);
                captured(txn.commit(&cx).await.unwrap())
            };
            assert_eq!(image.get_page(hot).unwrap().as_bytes()[0], 0x82);
            assert_live(&owner, &image).await;
        });
    }

    #[test]
    fn peer_after_commit_is_capture_failure_not_rollback_or_wrong_history() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut owner = owner().await;
            let hot = {
                let mut txn = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                let hot = append(&mut txn, 0x91).await;
                captured(txn.commit(&cx).await.unwrap());
                hot
            };
            {
                let mut txn = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                overwrite(&mut txn, hot, 0x92).await;
                // Pause the same protocol at the post-commit await boundary;
                // both commits below run through the real pager, not a mock.
                txn.attempt = Some(CommitAttempt {
                    pages: txn.inner.pending_commit_pages().unwrap(), has_writes: true,
                });
                txn.owner.current = None;
                txn.inner.commit(&cx).await.unwrap();
                {
                    let mut peer = txn.owner.pager.begin(&cx, TransactionMode::Immediate).await.unwrap();
                    peer.write_page(&cx, hot, &vec![0x93; PageSize::DEFAULT.as_usize()]).await.unwrap();
                    peer.commit(&cx).await.unwrap();
                }
                assert!(matches!(txn.finish_committed(&cx).await,
                    MemorySnapshotCommit::CaptureFailed(FrankenError::BusySnapshot { .. })));
                assert!(txn.owner.current.is_none());
                assert!(txn.rollback(&cx).await.is_err());
                assert!(txn.transaction_mut().is_err());
            }
            let image = {
                let mut txn = owner.begin(&cx, TransactionMode::ReadOnly).await.unwrap();
                assert!(txn.baseline_capture_stats().full_capture);
                captured(txn.commit(&cx).await.unwrap())
            };
            assert_eq!(image.get_page(hot).unwrap().as_bytes()[0], 0x93);
            assert_live(&owner, &image).await;
        });
    }

    #[test]
    fn settlement_finishes_the_same_committed_attempt_without_another_write() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut owner = owner().await;
            let (hot, before) = {
                let mut txn = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                let hot = append(&mut txn, 0x94).await;
                (hot, captured(txn.commit(&cx).await.unwrap()))
            };
            let after = {
                let mut txn = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                overwrite(&mut txn, hot, 0x95).await;
                // Stop at the real physical commit / history capture boundary.
                txn.attempt = Some(CommitAttempt {
                    pages: txn.inner.pending_commit_pages().unwrap(),
                    has_writes: true,
                });
                txn.owner.current = None;
                txn.inner.commit(&cx).await.unwrap();
                assert!(txn.transaction_mut().is_err());
                assert!(txn.rollback(&cx).await.is_err());
                match txn.settle_commit(&cx).await.unwrap() {
                    MemorySnapshotSettlement::Committed(result) => captured(result),
                    state => panic!("settling an actual committed handle: {state:?}"),
                }
            };
            assert_eq!(after.sequence().get(), before.sequence().get() + 1);
            assert_eq!(before.get_page(hot).unwrap().as_bytes()[0], 0x94);
            assert_eq!(after.get_page(hot).unwrap().as_bytes()[0], 0x95);
            assert_eq!(after.capture_stats().pages_read, 2);
            assert_live(&owner, &after).await;
        });
    }

    #[test]
    fn unpolled_commit_is_lazy_and_concurrent_mode_is_not_reinterpreted() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut owner = owner().await;
            assert!(matches!(
                owner.begin(&cx, TransactionMode::Concurrent).await,
                Err(FrankenError::Unsupported)
            ));
            assert!(owner.current().is_none());
            let image = {
                let mut txn = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                append(&mut txn, 0xa1).await;
                let unpolled = txn.commit(&cx);
                drop(unpolled);
                assert!(txn.attempt.is_none());
                assert!(!txn.finished);
                assert!(txn.transaction_mut().is_ok());
                captured(txn.commit(&cx).await.unwrap())
            };
            assert_live(&owner, &image).await;
        });
    }

    #[test]
    fn failed_physical_commit_keeps_recovery_handle_and_publishes_nothing() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let bytes = PageSize::DEFAULT.as_usize();
            let vfs = MemoryVfs::new_with_config(MemoryVfsConfig {
                initial_reserve_bytes: bytes, growth_chunk_bytes: bytes, max_bytes: Some(bytes),
            });
            let pager = SimplePager::open(vfs, Path::new("/limited.db"), PageSize::DEFAULT).await.unwrap();
            let mut owner = MemoryPageSnapshots::new(pager);
            let before = {
                let mut txn = owner.begin(&cx, TransactionMode::ReadOnly).await.unwrap();
                captured(txn.commit(&cx).await.unwrap())
            };
            {
                let mut txn = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                append(&mut txn, 0xaf).await;
                assert!(txn.commit(&cx).await.is_err(), "physical memory cap must reject growth");
                assert!(txn.owner.current.is_none());
                assert!(txn.transaction_mut().is_err(), "failed attempt needs reconciliation");
                assert!(matches!(txn.settle_commit(&cx).await.unwrap(), MemorySnapshotSettlement::NotCommitted));
                txn.rollback(&cx).await.unwrap();
            }
            assert_eq!(owner.current().unwrap().sequence(), before.sequence());
            assert_live(&owner, &before).await;
        });
    }

    #[test]
    fn capture_work_stays_bounded_with_32_mib_of_unrelated_resident_pages() {
        asupersync::test_utils::run_test(|| async {
            let cx = Cx::new();
            let mut owner = owner().await;
            let (hot, cold, seed) = {
                let mut txn = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                let hot = append(&mut txn, 0x01).await;
                let cold = append(&mut txn, 0x02).await;
                (hot, cold, captured(txn.commit(&cx).await.unwrap()))
            };
            let mut resident = 0;
            let mut history = std::collections::VecDeque::from([seed]);
            for target in [0, 128, 512, 2048, 8192] {
                if target > resident {
                    let mut txn = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                    for _ in resident..target { append(&mut txn, 0xb0).await; }
                    let image = captured(txn.commit(&cx).await.unwrap());
                    history.push_back(image);
                    resident = target;
                }
                for sample in 0..101 {
                    let old = owner.current().unwrap().clone();
                    let image = {
                        let mut txn = owner.begin(&cx, TransactionMode::Immediate).await.unwrap();
                        assert_eq!(txn.baseline_capture_stats().pages_read, 0, "resident={resident}");
                        overwrite(&mut txn, hot, (sample % 100 + 3) as u8).await;
                        generic_commit(txn.transaction_mut().unwrap(), &cx).await;
                        txn.owner.current.as_ref().unwrap().clone()
                    };
                    assert_eq!(image.capture_stats(), PageImageCaptureStats {
                        full_capture: false, pages_read: 2,
                    }, "resident={resident} sample={sample}");
                    assert_eq!(image.get_page(hot).unwrap().as_bytes()[0], (sample % 100 + 3) as u8);
                    assert!(std::ptr::eq(old.get_page(cold).unwrap(), image.get_page(cold).unwrap()));
                    history.push_back(image);
                    if history.len() > 16 { history.pop_front(); }
                }
                assert_live(&owner, owner.current().unwrap()).await;
            }
            assert_eq!(resident * PageSize::DEFAULT.as_usize(), 32 * 1024 * 1024);
        });
    }
}
