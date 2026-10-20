//! Caller-owned rewrite-before-sync recovery for native streams.
//!
//! A read/parse/checksum pass and a second fsync cannot repair a failed
//! writeback that left clean pages in the OS cache. This owner re-dirties the
//! exact retained bytes before either barrier. It is deliberately not hidden
//! inside the consuming `NativeDurabilityLog::recover` future: cancellation
//! must leave the write completion AND the original rewrite buffer accessible.

use std::fmt;
use std::sync::Arc;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::{CommitSeq, ObjectId, SymbolRecord};
use fsqlite_vfs::{FileIdentity, SyncKind, VfsFile, VfsWriteCompletion, VfsWriteCompletionState};

use super::{
    NativeDurabilityLimits, NativeDurabilityLog, NativeDurabilityRecovery, checkpoint, corrupt,
    ensure_distinct_files, read_exact_at, u64_len, zeroed,
};

const REWRITE_CHUNK_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy)]
enum Stream {
    Symbols,
    Markers,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct StreamBinding {
    symbols: FileIdentity,
    markers: FileIdentity,
    symbol_end: u64,
    marker_end: u64,
}

impl StreamBinding {
    fn capture<S: VfsFile, M: VfsFile>(log: &NativeDurabilityLog<S, M>, cx: &Cx) -> Result<Self> {
        let binding = Self {
            symbols: log
                .symbols
                .refresh_file_identity()?
                .ok_or(FrankenError::Unsupported)?,
            markers: log
                .markers
                .refresh_file_identity()?
                .ok_or(FrankenError::Unsupported)?,
            symbol_end: log.symbols.file_size(cx)?,
            marker_end: log.markers.file_size(cx)?,
        };
        if binding.symbols == binding.markers {
            return Err(corrupt("native recovery streams alias the same file"));
        }
        Ok(binding)
    }

    fn check<S: VfsFile, M: VfsFile>(
        &self,
        log: &NativeDurabilityLog<S, M>,
        cx: &Cx,
    ) -> Result<()> {
        if *self != Self::capture(log, cx)? {
            return Err(FrankenError::BusyRecovery);
        }
        Ok(())
    }
}

struct PendingRewrite {
    stream: Stream,
    offset: u64,
    bytes: Vec<u8>,
    binding: StreamBinding,
}

// Keep this outside the scanner's mutable append state. A valid shorter or
// substituted on-disk chain is not a recovery of a prefix already acknowledged
// by this owner. Its terminal marker digest binds the entire preceding chain.
#[derive(Clone, Copy)]
struct AcknowledgedPrefix {
    tip: CommitSeq,
    marker: Option<ObjectId>,
}

impl AcknowledgedPrefix {
    fn capture<S: VfsFile, M: VfsFile>(log: &NativeDurabilityLog<S, M>) -> Self {
        Self {
            tip: log.published_tip,
            marker: log.previous_marker,
        }
    }

    fn check(self, report: &NativeDurabilityRecovery) -> Result<()> {
        if self.tip == CommitSeq::ZERO {
            return if self.marker.is_none() {
                Ok(())
            } else {
                Err(corrupt("invalid acknowledged native genesis prefix"))
            };
        }
        let index = usize::try_from(self.tip.get() - 1).map_err(|_| FrankenError::TooBig)?;
        let marker = report
            .markers
            .get(index)
            .ok_or_else(|| corrupt("native recovery lost an acknowledged commit prefix"))?;
        if marker.commit_seq != self.tip
            || self.marker
                != Some(ObjectId::derive_from_canonical_bytes(
                    &marker.to_record_bytes(),
                ))
        {
            return Err(corrupt(
                "native recovery replaced an acknowledged commit prefix",
            ));
        }
        Ok(())
    }
}

/// Explicit recovery owner for a failed or newly reopened native log.
///
/// Keep this object AND the external append/namespace lease alive until its
/// tracked writes settle. Dropping `prepare` only abandons the wait; it does
/// not drop this owner, lose the source token, or forget the bytes needed to
/// retry a partial overwrite. No task/runtime is created. New handles require
/// the caller to settle every old owner's outstanding write before admission.
///
/// Recovery copies bytes in place; it never deletes, truncates, changes a
/// logical record, invents a commit, or treats a torn tail as appendable.
/// A complete corrupt marker or an undecodable committed referent still fails
/// closed. This is writeback recovery, not reconstruction of lost source bytes.
#[must_use]
pub struct NativeRecovery<S: VfsFile, M: VfsFile> {
    log: NativeDurabilityLog<S, M>,
    // Only a live publisher can attach its private ticket epoch. Fresh opens
    // and raw log copies cannot resolve another publisher's uncertain attempts.
    attempt_epoch: Option<Arc<()>>,
    acknowledged: AcknowledgedPrefix,
    pending: Option<PendingRewrite>,
    completed: Option<NativeDurabilityRecovery>,
}

impl<S: VfsFile, M: VfsFile> fmt::Debug for NativeRecovery<S, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeRecovery")
            .field("acknowledged_tip", &self.acknowledged.tip)
            .field("completed", &self.completed.is_some())
            .field("retained_rewrite", &self.pending.is_some())
            .field("write_pending", &self.write_pending())
            .finish_non_exhaustive()
    }
}

impl<S: VfsFile, M: VfsFile> NativeRecovery<S, M> {
    /// Admit already-open streams without writing them or claiming recovery.
    /// The original namespace/append lease must cover the entire session.
    ///
    /// # Errors
    /// Refuses invalid limits, cancellation, aliased/unidentifiable streams.
    pub fn new(cx: &Cx, symbols: S, markers: M, limits: NativeDurabilityLimits) -> Result<Self> {
        limits.validate()?;
        checkpoint(cx)?;
        ensure_distinct_files(&symbols, &markers)?;
        Ok(Self::from_log(NativeDurabilityLog::empty(
            symbols, markers, limits,
        )))
    }

    pub(super) fn from_log(mut log: NativeDurabilityLog<S, M>) -> Self {
        let acknowledged = AcknowledgedPrefix::capture(&log);
        log.poisoned = true;
        Self {
            log,
            attempt_epoch: None,
            acknowledged,
            pending: None,
            completed: None,
        }
    }

    pub(crate) fn with_attempt_epoch(mut self, epoch: Option<Arc<()>>) -> Self {
        self.attempt_epoch = epoch;
        self
    }

    /// The original source-owned completion, including an abandoned overwrite.
    #[must_use]
    pub fn outstanding_write(&self) -> Option<VfsWriteCompletion> {
        self.log.outstanding_write()
    }

    fn write_pending(&self) -> bool {
        self.log
            .last_write
            .as_ref()
            .is_some_and(|write| write.state() == VfsWriteCompletionState::Pending)
    }

    /// Validate the complete prefix, rewrite symbols, sync them, then rewrite
    /// and sync markers. All rewriting uses at most one 64-KiB owned chunk
    /// (also bounded by `max_batch_bytes`), independent of the stream length.
    /// The normal scanner/decoder bounds still apply to validation metadata.
    ///
    /// `verify_object` has the same mandatory decode/authentication/transitive
    /// dependency obligations as `NativeDurabilityLog::publish`. Validation
    /// precedes the first new rewrite. Retrying a previously attempted partial
    /// rewrite first restores its retained original bytes, then validates the
    /// complete prefix again. Subsequent retries re-dirty the entire streams:
    /// a failed sync is never retried by issuing just another sync.
    ///
    /// An existing log's acknowledged marker prefix must survive unchanged.
    /// Scanning a valid shorter or substituted chain cannot erase that memory,
    /// even across failed or abandoned attempts. A valid extension is allowed;
    /// only successful completion of both barriers advances the retained prefix.
    /// Freshly opened handles have no prior acknowledgement to compare against.
    ///
    /// An error or dropped future retains ownership and blocks `finish`.
    /// Pending physical writes must settle before retry, scan, or close. A
    /// settled partial overwrite retains its original buffer until restored;
    /// close refuses to discard that recovery obligation. No cancellation
    /// check after the final successful barrier revokes a completed recovery.
    ///
    /// # Errors
    /// Propagates validation/I/O/resource errors or cancellation. Refuses
    /// outstanding writes, closed streams, or changed stream identities/sizes.
    pub async fn prepare<V, VF>(&mut self, cx: &Cx, verify_object: V) -> Result<()>
    where
        V: FnMut(ObjectId, Vec<SymbolRecord>) -> VF,
        VF: std::future::Future<Output = Result<()>>,
    {
        if self.write_pending() || self.log.symbols_closed || self.log.markers_closed {
            return Err(FrankenError::BusyRecovery);
        }
        checkpoint(cx)?;
        self.completed = None;
        self.log.poisoned = true;
        self.restore_pending(cx).await?;

        let binding = StreamBinding::capture(&self.log, cx)?;
        let report = self.log.scan_recovery(cx, verify_object).await?;
        binding.check(&self.log, cx)?;
        self.acknowledged.check(&report)?;
        self.rewrite_stream(cx, Stream::Symbols, binding.symbol_end, binding)
            .await?;
        checkpoint(cx)?;
        binding.check(&self.log, cx)?;
        self.log.symbols.durable_sync(cx, SyncKind::FullDurable)?;

        self.rewrite_stream(cx, Stream::Markers, binding.marker_end, binding)
            .await?;
        checkpoint(cx)?;
        binding.check(&self.log, cx)?;
        self.log.markers.durable_sync(cx, SyncKind::FullDurable)?;
        // Do not turn an acknowledged barrier into a retry due to late cancel.
        self.acknowledged = AcknowledgedPrefix::capture(&self.log);
        self.log.last_write = None;
        self.log.poisoned = false;
        self.completed = Some(report);
        Ok(())
    }

    async fn rewrite_stream(
        &mut self,
        cx: &Cx,
        stream: Stream,
        end: u64,
        binding: StreamBinding,
    ) -> Result<()> {
        let chunk_size = self.log.limits.max_batch_bytes.min(REWRITE_CHUNK_BYTES);
        let mut offset = 0;
        while offset < end {
            checkpoint(cx)?;
            binding.check(&self.log, cx)?;
            let count = usize::try_from((end - offset).min(u64_len(chunk_size)?))
                .map_err(|_| FrankenError::TooBig)?;
            let mut bytes = zeroed(count)?;
            match stream {
                Stream::Symbols => read_exact_at(&self.log.symbols, cx, &mut bytes, offset).await?,
                Stream::Markers => read_exact_at(&self.log.markers, cx, &mut bytes, offset).await?,
            }
            // Retain BEFORE polling the source mutation. A failed/abandoned
            // write may have damaged the readable copy at this exact offset.
            self.pending = Some(PendingRewrite {
                stream,
                offset,
                bytes,
                binding,
            });
            self.restore_pending(cx).await?;
            offset += u64_len(count)?; // count is bounded by end - offset.
        }
        Ok(())
    }

    async fn restore_pending(&mut self, cx: &Cx) -> Result<()> {
        if self.write_pending() {
            return Err(FrankenError::BusyRecovery);
        }
        let Some(pending) = &self.pending else {
            return Ok(());
        };
        checkpoint(cx)?;
        pending.binding.check(&self.log, cx)?;
        let completion = VfsWriteCompletion::new();
        self.log.last_write = Some(completion.clone());
        match pending.stream {
            Stream::Symbols => {
                self.log
                    .symbols
                    .write_tracked(cx, &pending.bytes, pending.offset, completion)
                    .await?;
            }
            Stream::Markers => {
                self.log
                    .markers
                    .write_tracked(cx, &pending.bytes, pending.offset, completion)
                    .await?;
            }
        }
        self.pending = None;
        self.log.last_write = None;
        Ok(())
    }

    /// Transfer only a completely recovered log. On refusal the entire owner
    /// is returned, including any buffer/token needed to finish recovery.
    /// Torn tails are transferable but remain non-appendable in the log.
    ///
    /// # Errors
    /// Returns `self` until prepare succeeds, or after close has begun.
    #[allow(clippy::result_large_err)] // Returning the owner preserves physical write obligations.
    pub fn finish(mut self) -> std::result::Result<RecoveredNativeLog<S, M>, Self> {
        if self.log.poisoned
            || self.pending.is_some()
            || self.write_pending()
            || self.log.symbols_closed
            || self.log.markers_closed
        {
            return Err(self);
        }
        let Some(report) = self.completed.take() else {
            return Err(self);
        };
        Ok(RecoveredNativeLog {
            log: self.log,
            report,
            attempt_epoch: self.attempt_epoch,
        })
    }

    /// Close only when no original rewrite bytes or physical write are owed.
    ///
    /// # Errors
    /// Returns BusyRecovery for a retained overwrite; preserves close failures.
    pub fn close(&mut self, cx: &Cx) -> Result<()> {
        if self.pending.is_some() {
            return Err(FrankenError::BusyRecovery);
        }
        self.completed = None;
        self.log.close(cx)
    }
}

/// Unforgeable handoff from a completed rewrite and both durability barriers.
///
/// Owning it does not replace the external namespace/append lease or SQL/SSI
/// validation, and its report does not acknowledge a new commit to old callers.
#[must_use]
pub struct RecoveredNativeLog<S: VfsFile, M: VfsFile> {
    log: NativeDurabilityLog<S, M>,
    report: NativeDurabilityRecovery,
    attempt_epoch: Option<Arc<()>>,
}

impl<S: VfsFile, M: VfsFile> fmt::Debug for RecoveredNativeLog<S, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecoveredNativeLog")
            .field("report", &self.report)
            .finish_non_exhaustive()
    }
}

impl<S: VfsFile, M: VfsFile> RecoveredNativeLog<S, M> {
    // Only retained semantic recovery receives the originating ticket epoch.
    // The public raw-log escape intentionally drops that lineage: installing
    // any new publisher starts a new epoch, even on the same physical files.
    pub(crate) fn into_reconciliation_parts(
        self,
    ) -> (
        NativeDurabilityLog<S, M>,
        NativeDurabilityRecovery,
        Option<Arc<()>>,
    ) {
        (self.log, self.report, self.attempt_epoch)
    }

    /// Verified committed prefix and preserved tail accounting.
    #[must_use]
    pub const fn report(&self) -> &NativeDurabilityRecovery {
        &self.report
    }

    /// Resume low-level log use without reopening either physical handle.
    /// Higher layers must independently reconstruct their semantic state.
    #[must_use]
    pub fn into_log(self) -> (NativeDurabilityLog<S, M>, NativeDurabilityRecovery) {
        (self.log, self.report)
    }
}

impl<S: VfsFile, M: VfsFile> NativeDurabilityLog<S, M> {
    // Outcome queries must not mistake a stale, shorter in-memory report for
    // absence after an independently changed stream. The caller still owns
    // namespace/append exclusion; this check does not replace that authority.
    pub(crate) fn validate_recovered_lengths(&self, cx: &Cx) -> Result<()> {
        self.readable()?;
        if self.symbols.file_size(cx)? != self.symbol_end
            || self.markers.file_size(cx)? != self.marker_end
        {
            return Err(FrankenError::BusyRecovery);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod acknowledged_prefix_tests {
    use std::future::{Future, pending, poll_fn, ready};
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::Poll;

    use asupersync::runtime::RuntimeBuilder;
    use fsqlite_types::flags::{SyncFlags, VfsOpenFlags};
    use fsqlite_types::{
        CommitMarker, LockLevel, Oti, SymbolRecordFlags, reconstruct_systematic_happy_path,
    };
    use fsqlite_vfs::{MemoryVfs, ShmRegion, Vfs};

    use super::*;

    #[derive(Default)]
    struct Audit {
        writes: AtomicUsize,
        syncs: AtomicUsize,
        deny: AtomicBool,
        fail_sync: AtomicUsize,
    }

    impl Audit {
        fn reset(&self, deny: bool) {
            self.writes.store(0, Ordering::SeqCst);
            self.syncs.store(0, Ordering::SeqCst);
            self.fail_sync.store(0, Ordering::SeqCst);
            self.deny.store(deny, Ordering::SeqCst);
        }

        fn mutation(&self, sync: bool) -> Result<()> {
            let count = if sync { &self.syncs } else { &self.writes };
            let ordinal = count.fetch_add(1, Ordering::SeqCst) + 1;
            if self.deny.load(Ordering::SeqCst)
                || (sync && self.fail_sync.load(Ordering::SeqCst) == ordinal)
            {
                return Err(std::io::Error::other("injected recovery mutation failure").into());
            }
            Ok(())
        }

        fn assert_no_mutations(&self) {
            assert_eq!(self.writes.load(Ordering::SeqCst), 0);
            assert_eq!(self.syncs.load(Ordering::SeqCst), 0);
        }
    }

    struct File {
        inner: <MemoryVfs as Vfs>::File,
        audit: Arc<Audit>,
    }

    impl VfsFile for File {
        fn close(&mut self, cx: &Cx) -> Result<()> {
            self.inner.close(cx)
        }
        fn file_identity(&self) -> Result<Option<FileIdentity>> {
            self.inner.file_identity()
        }
        fn refresh_file_identity(&self) -> Result<Option<FileIdentity>> {
            self.inner.refresh_file_identity()
        }
        async fn read<'a>(&'a self, cx: &'a Cx, bytes: &'a mut [u8], offset: u64) -> Result<usize> {
            self.inner.read(cx, bytes, offset).await
        }
        async fn write<'a>(&'a self, cx: &'a Cx, bytes: &'a [u8], offset: u64) -> Result<()> {
            self.audit.mutation(false)?;
            self.inner.write(cx, bytes, offset).await
        }
        async fn write_tracked<'a>(
            &'a self,
            cx: &'a Cx,
            bytes: &'a [u8],
            offset: u64,
            completion: VfsWriteCompletion,
        ) -> Result<()> {
            if let Err(error) = self.audit.mutation(false) {
                completion.complete_error();
                return Err(error);
            }
            self.inner
                .write_tracked(cx, bytes, offset, completion)
                .await
        }
        fn truncate(&mut self, cx: &Cx, size: u64) -> Result<()> {
            self.audit.mutation(false)?;
            self.inner.truncate(cx, size)
        }
        fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()> {
            self.audit.mutation(true)?;
            self.inner.sync(cx, flags)
        }
        fn durable_sync(&mut self, cx: &Cx, kind: SyncKind) -> Result<()> {
            self.audit.mutation(true)?;
            self.inner.durable_sync(cx, kind)
        }
        fn file_size(&self, cx: &Cx) -> Result<u64> {
            self.inner.file_size(cx)
        }
        fn lock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> {
            self.inner.lock(cx, level)
        }
        fn unlock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> {
            self.inner.unlock(cx, level)
        }
        fn lock_external_wal_append(&mut self, cx: &Cx) -> Result<()> {
            self.inner.lock_external_wal_append(cx)
        }
        fn owns_external_wal_append_write(&self, cx: &Cx) -> Result<bool> {
            self.inner.owns_external_wal_append_write(cx)
        }
        fn restore_external_wal_append_attempt(&mut self, cx: &Cx) -> Result<()> {
            self.inner.restore_external_wal_append_attempt(cx)
        }
        fn lock_external_shared_snapshot(&mut self, cx: &Cx) -> Result<()> {
            self.inner.lock_external_shared_snapshot(cx)
        }
        fn restore_external_shared_snapshot_attempt(&mut self, cx: &Cx) -> Result<()> {
            self.inner.restore_external_shared_snapshot_attempt(cx)
        }
        fn lock_external_maintenance(&mut self, cx: &Cx, wal: bool) -> Result<()> {
            self.inner.lock_external_maintenance(cx, wal)
        }
        fn lock_external_wal_recovery(&mut self, cx: &Cx) -> Result<()> {
            self.inner.lock_external_wal_recovery(cx)
        }
        fn restore_external_maintenance_attempt(&mut self, cx: &Cx) -> Result<()> {
            self.inner.restore_external_maintenance_attempt(cx)
        }
        fn check_reserved_lock(&self, cx: &Cx) -> Result<bool> {
            self.inner.check_reserved_lock(cx)
        }
        fn shm_map(&mut self, cx: &Cx, region: u32, size: u32, extend: bool) -> Result<ShmRegion> {
            self.inner.shm_map(cx, region, size, extend)
        }
        fn shm_lock(&mut self, cx: &Cx, offset: u32, n: u32, flags: u32) -> Result<()> {
            self.inner.shm_lock(cx, offset, n, flags)
        }
        fn shm_barrier(&self) {
            self.inner.shm_barrier();
        }
        fn shm_unmap(&mut self, cx: &Cx, delete: bool) -> Result<()> {
            self.inner.shm_unmap(cx, delete)
        }
    }

    type Log = NativeDurabilityLog<File, File>;
    type Recovery = NativeRecovery<File, File>;

    fn run(future: impl Future<Output = ()>) {
        RuntimeBuilder::current_thread()
            .blocking_threads(1, 2)
            .build()
            .unwrap()
            .block_on(future);
    }

    fn raw(vfs: &MemoryVfs, cx: &Cx, name: &str) -> <MemoryVfs as Vfs>::File {
        vfs.open(
            cx,
            Some(Path::new(name)),
            VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL,
        )
        .unwrap()
        .0
    }

    fn file(vfs: &MemoryVfs, cx: &Cx, name: &str, audit: &Arc<Audit>) -> File {
        File {
            inner: raw(vfs, cx, name),
            audit: Arc::clone(audit),
        }
    }

    fn symbol(value: u8) -> SymbolRecord {
        let bytes = vec![value; 32];
        SymbolRecord::new(
            ObjectId::derive_from_canonical_bytes(&bytes),
            Oti {
                f: 32,
                al: 1,
                t: 32,
                z: 1,
                n: 1,
            },
            0,
            bytes,
            SymbolRecordFlags::SYSTEMATIC_RUN_START,
        )
    }

    fn verify(id: ObjectId, records: Vec<SymbolRecord>) -> std::future::Ready<Result<()>> {
        ready(
            reconstruct_systematic_happy_path(&records)
                .map_err(|error| corrupt(&error.to_string()))
                .and_then(|bytes| {
                    if ObjectId::derive_from_canonical_bytes(&bytes) == id {
                        Ok(())
                    } else {
                        Err(corrupt("test recovery object mismatch"))
                    }
                }),
        )
    }

    fn chain(times: &[u64]) -> Vec<CommitMarker> {
        let mut previous = None;
        times
            .iter()
            .enumerate()
            .map(|(index, time)| {
                let marker = CommitMarker::new(
                    CommitSeq::new(u64::try_from(index).unwrap() + 1),
                    *time,
                    symbol(1).object_id,
                    symbol(2).object_id,
                    previous,
                );
                previous = Some(ObjectId::derive_from_canonical_bytes(
                    &marker.to_record_bytes(),
                ));
                marker
            })
            .collect()
    }

    async fn log(vfs: &MemoryVfs, cx: &Cx, audit: &Arc<Audit>, times: &[u64]) -> Log {
        let mut log = Log::create(
            cx,
            file(vfs, cx, "objects", audit),
            file(vfs, cx, "markers", audit),
            NativeDurabilityLimits::default(),
        )
        .unwrap();
        log.append_symbols(cx, &[symbol(1), symbol(2)])
            .await
            .unwrap();
        log.publish(cx, &chain(times), verify).await.unwrap();
        log
    }

    // Corrupt only this test's MemoryVfs bytes, outside the audited recovery
    // handles. Truncation/replacement must be detected before recovery writes.
    async fn set_markers(vfs: &MemoryVfs, cx: &Cx, markers: &[CommitMarker]) {
        let bytes: Vec<_> = markers
            .iter()
            .flat_map(|marker| marker.to_record_bytes())
            .collect();
        let mut file = raw(vfs, cx, "markers");
        file.truncate(cx, 0).unwrap();
        file.write(cx, &bytes, 0).await.unwrap();
        file.close(cx).unwrap();
    }

    async fn marker_bytes(vfs: &MemoryVfs, cx: &Cx) -> Vec<u8> {
        let mut file = raw(vfs, cx, "markers");
        let mut bytes = vec![0; usize::try_from(file.file_size(cx).unwrap()).unwrap()];
        assert_eq!(file.read(cx, &mut bytes, 0).await.unwrap(), bytes.len());
        file.close(cx).unwrap();
        bytes
    }

    fn refused(owner: Recovery) -> Recovery {
        owner
            .finish()
            .expect_err("a refused recovery must not produce a sealed handoff")
    }

    #[test]
    fn acknowledged_truncation_is_not_reclassified_as_an_uncommitted_tail() {
        run(async {
            for times in [&[][..], &[100][..]] {
                let cx = Cx::new();
                let vfs = MemoryVfs::new();
                let audit = Arc::new(Audit::default());
                let mut owner = log(&vfs, &cx, &audit, &[100, 200]).await.into_recovery();
                set_markers(&vfs, &cx, &chain(times)).await;
                let damaged = marker_bytes(&vfs, &cx).await;
                audit.reset(true);
                for _ in 0..2 {
                    let error = owner.prepare(&cx, verify).await.unwrap_err();
                    assert!(matches!(error, FrankenError::WalCorrupt { .. }));
                    assert!(error.to_string().contains("lost an acknowledged"));
                    assert_eq!(owner.acknowledged.tip, CommitSeq::new(2));
                    owner = refused(owner);
                    audit.assert_no_mutations();
                    assert_eq!(marker_bytes(&vfs, &cx).await, damaged);
                }
                // Restoring the original retained prefix permits ordinary recovery.
                set_markers(&vfs, &cx, &chain(&[100, 200])).await;
                audit.reset(false);
                owner.prepare(&cx, verify).await.unwrap();
                let (mut log, report) = owner.finish().unwrap().into_log();
                assert_eq!(report.markers.len(), 2);
                assert_eq!(log.published_tip(), CommitSeq::new(2));
                log.close(&cx).unwrap();
            }
        });
    }

    #[test]
    fn acknowledged_chain_identity_survives_same_length_substitution_and_retries() {
        run(async {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let audit = Arc::new(Audit::default());
            let mut owner = log(&vfs, &cx, &audit, &[100, 200]).await.into_recovery();
            let expected = owner.acknowledged.marker;
            for times in [[101, 200], [100, 201], [101, 201]] {
                set_markers(&vfs, &cx, &chain(&times)).await;
                audit.reset(true);
                let error = owner.prepare(&cx, verify).await.unwrap_err();
                assert!(matches!(error, FrankenError::WalCorrupt { .. }));
                assert!(error.to_string().contains("replaced an acknowledged"));
                assert_eq!(owner.acknowledged.marker, expected);
                owner = refused(owner);
                audit.assert_no_mutations();
            }
            owner.close(&cx).unwrap();
        });
    }

    #[test]
    fn valid_extension_advances_the_anchor_only_after_both_recovery_barriers() {
        run(async {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let audit = Arc::new(Audit::default());
            let mut owner = log(&vfs, &cx, &audit, &[100]).await.into_recovery();
            set_markers(&vfs, &cx, &chain(&[100, 200])).await;
            audit.reset(false);
            owner.prepare(&cx, verify).await.unwrap();
            assert_eq!(audit.syncs.load(Ordering::SeqCst), 2);
            assert_eq!(owner.acknowledged.tip, CommitSeq::new(2));
            set_markers(&vfs, &cx, &chain(&[100])).await;
            audit.reset(true);
            assert!(matches!(
                owner.prepare(&cx, verify).await,
                Err(FrankenError::WalCorrupt { .. })
            ));
            owner = refused(owner);
            audit.assert_no_mutations();
            set_markers(&vfs, &cx, &chain(&[100, 200, 300])).await;
            audit.reset(false);
            owner.prepare(&cx, verify).await.unwrap();
            let (mut log, report) = owner.finish().unwrap().into_log();
            assert_eq!(report.markers.len(), 3);
            let extended = chain(&[100, 200, 300, 400]);
            log.publish(&cx, &extended[3..], verify).await.unwrap();
            assert_eq!(log.published_tip(), CommitSeq::new(4));
            log.close(&cx).unwrap();
        });
    }

    #[test]
    fn failed_barriers_do_not_turn_a_tentative_extension_into_an_acknowledgement() {
        run(async {
            for barrier in [1, 2] {
                let cx = Cx::new();
                let vfs = MemoryVfs::new();
                let audit = Arc::new(Audit::default());
                let mut owner = log(&vfs, &cx, &audit, &[100]).await.into_recovery();
                set_markers(&vfs, &cx, &chain(&[100, 200])).await;
                audit.reset(false);
                audit.fail_sync.store(barrier, Ordering::SeqCst);
                assert!(owner.prepare(&cx, verify).await.is_err());
                assert_eq!(audit.syncs.load(Ordering::SeqCst), barrier);
                assert_eq!(owner.acknowledged.tip, CommitSeq::new(1));
                owner = refused(owner);
                // Model loss of the unacknowledged tail, not of the old prefix.
                set_markers(&vfs, &cx, &chain(&[100])).await;
                audit.reset(false);
                owner.prepare(&cx, verify).await.unwrap();
                assert_eq!(audit.syncs.load(Ordering::SeqCst), 2);
                let (mut log, report) = owner.finish().unwrap().into_log();
                assert_eq!(report.markers.len(), 1);
                assert!(!log.needs_recovery());
                log.close(&cx).unwrap();
            }
        });
    }

    #[test]
    fn fresh_recovery_establishes_an_anchor_and_still_preserves_torn_tails() {
        run(async {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let audit = Arc::new(Audit::default());
            log(&vfs, &cx, &audit, &[100, 200])
                .await
                .close(&cx)
                .unwrap();
            let mut tail = raw(&vfs, &cx, "markers");
            let end = tail.file_size(&cx).unwrap();
            tail.write(&cx, b"torn", end).await.unwrap();
            tail.close(&cx).unwrap();
            let original = marker_bytes(&vfs, &cx).await;
            let mut owner = Recovery::new(
                &cx,
                file(&vfs, &cx, "objects", &audit),
                file(&vfs, &cx, "markers", &audit),
                NativeDurabilityLimits::default(),
            )
            .unwrap();
            assert_eq!(owner.acknowledged.tip, CommitSeq::ZERO);
            audit.reset(false);
            owner.prepare(&cx, verify).await.unwrap();
            assert_eq!(owner.acknowledged.tip, CommitSeq::new(2));
            assert_eq!(owner.completed.as_ref().unwrap().marker_tail_bytes, 4);
            assert_eq!(marker_bytes(&vfs, &cx).await, original);
            set_markers(&vfs, &cx, &chain(&[100])).await;
            audit.reset(true);
            assert!(matches!(
                owner.prepare(&cx, verify).await,
                Err(FrankenError::WalCorrupt { .. })
            ));
            owner = refused(owner);
            audit.assert_no_mutations();
            owner.close(&cx).unwrap();
        });
    }

    #[test]
    fn abandoned_and_cancelled_scans_keep_the_original_acknowledged_prefix() {
        run(async {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let audit = Arc::new(Audit::default());
            let mut owner = log(&vfs, &cx, &audit, &[100, 200]).await.into_recovery();
            audit.reset(true);
            {
                let mut future = Box::pin(owner.prepare(&cx, |_, _| pending::<Result<()>>()));
                poll_fn(|task_cx| {
                    assert!(future.as_mut().poll(task_cx).is_pending());
                    Poll::Ready(())
                })
                .await;
            }
            owner = refused(owner);
            let cancelled = Cx::new();
            cancelled.cancel();
            assert!(matches!(
                owner.prepare(&cancelled, verify).await,
                Err(FrankenError::Interrupt)
            ));
            owner = refused(owner);
            set_markers(&vfs, &cx, &chain(&[100])).await;
            assert!(matches!(
                owner.prepare(&cx, verify).await,
                Err(FrankenError::WalCorrupt { .. })
            ));
            owner = refused(owner);
            assert_eq!(owner.acknowledged.tip, CommitSeq::new(2));
            audit.assert_no_mutations();
            owner.close(&cx).unwrap();
        });
    }
}
