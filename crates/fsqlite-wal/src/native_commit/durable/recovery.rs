//! Retained semantic replay of a completed native storage recovery.
//!
//! This stage only reads: the sealed input already completed rewrite and both
//! durability barriers. Dropping a replay future keeps the physical handles in
//! this owner, and no partial conflict index is ever installed in a live driver.

mod outcome;
pub use outcome::{NativeCommitAttempt, NativeCommitOutcome};

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::{CommitSeq, ObjectId, OperatingMode};
use fsqlite_vfs::VfsFile;

use super::{
    DurableWriteCoordinator, MAX_NATIVE_VALIDATION_BYTES, NativeCommitCandidate,
    NativeCommitProof, NativeObjectCodec, WriteCoordinator, checkpoint, corrupt, validate_capacity,
};
use crate::native_durability::{
    NativeDurabilityLog, NativeDurabilityRecovery, NativeRecovery, RecoveredNativeLog,
};

impl<S: VfsFile, M: VfsFile, C: NativeObjectCodec> DurableWriteCoordinator<S, M, C> {
    /// Move a quiescent or failed publisher into physical recovery on its exact
    /// original streams. No close, reopen, write, sync or cancellation check is
    /// performed by this ownership transfer. The codec need not implement Clone.
    ///
    /// Drain completed acknowledgements with `take_committed` first, including
    /// successes preceding a later failed batch. A healthy unflushed queue must
    /// be completed, not silently discarded. Failed reservations may be left
    /// behind, but transferring them does NOT classify their commits as aborted:
    /// callers must reconcile their recorded identities with recovered history.
    ///
    /// The log carries its acknowledged-prefix fence and original outstanding
    /// write token into `NativeRecovery`. A pending write may be transferred,
    /// but recovery cannot read, rewrite or close until that source write settles.
    /// Retain the external append/namespace lease throughout the handoff.
    ///
    /// # Errors
    /// Returns the unchanged coordinator while closing, with an undelivered
    /// acknowledgement, or with healthy queued work. No files are changed.
    #[allow(clippy::result_large_err)] // Refusal must return every queue and storage obligation.
    pub fn try_into_recovery(self) -> std::result::Result<(NativeRecovery<S, M>, C), Self> {
        if self.closing
            || (!self.needs_recovery() && self.pending_count() != 0)
            || self.coordinator.batch.pending.iter().any(|pending| pending.barriers.all_complete())
        {
            return Err(self);
        }
        Ok((self.log.into_recovery().with_attempt_epoch(Some(self.attempt_epoch)), self.codec))
    }
}

/// Rebuild the commit sequencer and conflict index without reopening storage.
///
/// Keep the external namespace/append lease continuously held from storage
/// recovery through this handoff and subsequent driver use. The report is not
/// an acknowledgement to an earlier caller. Torn tails remain append-blocking.
/// Codec authentication and historical SSI validation remain mandatory; merely
/// possessing a storage receipt does not establish either semantic contract.
#[must_use]
pub struct NativeCoordinatorRecovery<S: VfsFile, M: VfsFile, C: NativeObjectCodec> {
    log: NativeDurabilityLog<S, M>,
    attempt_epoch: Option<Arc<()>>,
    report: NativeDurabilityRecovery,
    codec: C,
    max_pending: usize,
    prepared: Option<WriteCoordinator>,
    closing: bool,
}

impl<S: VfsFile, M: VfsFile, C: NativeObjectCodec> fmt::Debug
    for NativeCoordinatorRecovery<S, M, C>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeCoordinatorRecovery")
            .field("commits", &self.report.markers.len())
            .field("prepared", &self.prepared.is_some())
            .field("closing", &self.closing)
            .finish_non_exhaustive()
    }
}

impl<S: VfsFile, M: VfsFile, C: NativeObjectCodec> NativeCoordinatorRecovery<S, M, C> {
    /// Retain an unforgeable storage handoff. No I/O or replay occurs here.
    /// Queue bounds are checked by `prepare`, without consuming the owner.
    pub fn new(recovered: RecoveredNativeLog<S, M>, codec: C, max_pending: usize) -> Self {
        let (log, report, attempt_epoch) = recovered.into_reconciliation_parts();
        Self { log, attempt_epoch, report, codec, max_pending, prepared: None, closing: false }
    }

    /// The original storage recovery report, never a caller-supplied replay list.
    #[must_use]
    pub const fn report(&self) -> &NativeDurabilityRecovery {
        &self.report
    }

    /// Replay stored proofs, capsules and immediate evidence in commit order.
    ///
    /// A failed, cancelled, unwound or dropped attempt retains these handles
    /// but discards its partial sequencer. Every retry starts from genesis.
    /// The callback must bind the write set and validate historical semantics;
    /// external effects performed by a callback are NOT rolled back here.
    /// There are no writes, synchronization calls, opens, closes or new replies.
    ///
    /// # Errors
    /// Refuses closed ownership, invalid queue bounds, cancellation, unreadable
    /// objects, invalid proof bindings/digests, or a rejected semantic history.
    pub async fn prepare<V>(&mut self, cx: &Cx, validate: V) -> Result<()>
    where
        V: FnMut(&NativeCommitCandidate, &BTreeMap<ObjectId, Arc<[u8]>>) -> Result<()>,
    {
        self.prepared = None;
        if self.closing {
            return Err(FrankenError::BusyRecovery);
        }
        let coordinator = replay(
            cx, &self.log, &self.report, &self.codec, self.max_pending, validate,
        ).await?;
        self.prepared = Some(coordinator);
        Ok(())
    }

    // The native-page layer calls this only after its mandatory full-history
    // replay and snapshot allocation. Do not expose a public callback-free
    // generic semantic admission path. Destination publication re-reads and
    // authenticates its referents with this same codec; the lower transfer
    // retains its chain/alias checks, two barriers and failed-write ownership.
    pub(crate) async fn replicate_prepared_prefix_to<DS: VfsFile, DM: VfsFile>(
        &mut self,
        cx: &Cx,
        destination: &mut NativeDurabilityLog<DS, DM>,
        max_commits: usize,
    ) -> Result<Option<crate::native_durability::NativeDurabilityReceipt>> {
        if self.closing || self.prepared.is_none() {
            return Err(FrankenError::BusyRecovery);
        }
        let codec = &self.codec;
        self.log
            .replicate_committed_prefix_to(cx, destination, max_commits, |id, records| {
                std::future::ready(codec.decode(cx, id, &records).map(|_| ()))
            })
            .await
    }

    /// Install only a complete replay. Refusal returns the entire owner.
    /// No await or callback separates this handoff from driver construction.
    ///
    /// # Errors
    /// Returns `self` before successful preparation or after close has started.
    #[allow(clippy::result_large_err)] // Returning ownership preserves the exact storage handles.
    pub fn finish(mut self) -> std::result::Result<
        (DurableWriteCoordinator<S, M, C>, NativeDurabilityRecovery), Self,
    > {
        if self.closing {
            return Err(self);
        }
        let Some(coordinator) = self.prepared.take() else { return Err(self); };
        Ok((DurableWriteCoordinator::from_parts(coordinator, self.log, self.codec), self.report))
    }

    /// Return to explicit storage recovery without reopening or losing the codec.
    /// This invalidates the old replay and requires both durability barriers again.
    #[must_use]
    pub fn into_recovery(self) -> (NativeRecovery<S, M>, C) {
        (self.log.into_recovery().with_attempt_epoch(self.attempt_epoch), self.codec)
    }

    /// Close the exact retained handles; individual close failures remain retryable.
    ///
    /// # Errors
    /// Propagates the VFS close error. Replay cannot resume once closing begins.
    pub fn close(&mut self, cx: &Cx) -> Result<()> {
        self.prepared = None;
        self.closing = true;
        self.log.close(cx)
    }
}

// The ordinary fresh-boot entry and the sealed rewrite-recovery handoff share
// ONE proof interpreter. Neither can substitute report metadata for stored
// capsule bytes or silently omit the caller's historical semantic validator.
pub(super) async fn replay<S: VfsFile, M: VfsFile, C: NativeObjectCodec, V>(
    cx: &Cx,
    log: &NativeDurabilityLog<S, M>,
    report: &NativeDurabilityRecovery,
    codec: &C,
    max_pending: usize,
    mut validate: V,
) -> Result<WriteCoordinator>
where
    V: FnMut(&NativeCommitCandidate, &BTreeMap<ObjectId, Arc<[u8]>>) -> Result<()>,
{
    validate_capacity(max_pending)?;
    checkpoint(cx)?;
    let mut coordinator =
        WriteCoordinator::new(OperatingMode::Native, CommitSeq::ZERO, max_pending);
    for marker in &report.markers {
        checkpoint(cx)?;
        let proof_records = log.read_object(cx, marker.proof_object_id).await?;
        let proof_bytes = codec.decode(cx, marker.proof_object_id, &proof_records)?;
        let proof = NativeCommitProof::from_bytes(&proof_bytes)?;
        if proof.commit_seq != marker.commit_seq
            || proof.commit_time_unix_ns != marker.commit_time_unix_ns
            || proof.submission.capsule_object_id != marker.capsule_object_id
        {
            return Err(corrupt("native recovery proof is not bound to its marker"));
        }
        let mut objects = BTreeMap::<ObjectId, Arc<[u8]>>::new();
        let mut decoded_bytes = 0_usize;
        for id in std::iter::once(&proof.submission.capsule_object_id)
            .chain(&proof.submission.witness_refs)
            .chain(&proof.submission.edge_ids)
            .chain(&proof.submission.merge_witness_ids)
        {
            if objects.contains_key(id) {
                continue;
            }
            let records = log.read_object(cx, *id).await?;
            let payload = codec.decode(cx, *id, &records)?;
            decoded_bytes = decoded_bytes
                .checked_add(payload.len())
                .filter(|n| *n <= MAX_NATIVE_VALIDATION_BYTES)
                .ok_or(FrankenError::TooBig)?;
            objects.insert(*id, Arc::from(payload));
        }
        let capsule = Arc::clone(
            objects.get(&marker.capsule_object_id)
                .ok_or_else(|| corrupt("native recovery capsule is missing"))?,
        );
        if blake3::hash(&capsule).as_bytes() != &proof.submission.capsule_digest {
            return Err(corrupt("native recovery capsule digest mismatch"));
        }
        let candidate = NativeCommitCandidate {
            proof, proof_object_id: marker.proof_object_id, capsule,
        };
        validate(&candidate, &objects)?;
        coordinator.commit_index.record_commit(
            &candidate.proof.submission.write_set_pages, marker.commit_seq,
        );
        coordinator.commit_seq_tip = marker.commit_seq;
        coordinator.allocated_seq_tip = marker.commit_seq;
        coordinator.last_commit_time_ns = marker.commit_time_unix_ns;
        coordinator.prev_marker_id = Some(ObjectId::derive_from_canonical_bytes(
            &marker.to_record_bytes(),
        ));
    }
    if coordinator.commit_seq_tip != log.published_tip() {
        return Err(corrupt("native recovery coordinator/log tip mismatch"));
    }
    // This is read-only replay, not a physical commit. Cancellation inside the
    // last callback must not install a partial or unwanted reconstructed driver.
    checkpoint(cx)?;
    Ok(coordinator)
}

#[cfg(test)]
mod live_transfer_tests {
    use std::future::{Future, ready};
    use std::path::Path;

    use asupersync::runtime::RuntimeBuilder;
    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_types::{
        Oti, PageNumber, SymbolRecord, SymbolRecordFlags, TxnEpoch, TxnId, TxnToken,
        reconstruct_systematic_happy_path,
    };
    use fsqlite_vfs::{MemoryVfs, Vfs};

    use super::*;
    use crate::native_commit::CommitSubmission;
    use crate::native_durability::NativeDurabilityLimits;

    // Deliberately not Clone: transfer must preserve the original codec owner.
    struct Codec;
    impl NativeObjectCodec for Codec {
        fn encode(&self, _: &Cx, bytes: &[u8]) -> Result<Vec<SymbolRecord>> {
            let size = u32::try_from(bytes.len()).map_err(|_| FrankenError::TooBig)?;
            Ok(vec![SymbolRecord::new(
                ObjectId::derive_from_canonical_bytes(bytes),
                Oti { f: u64::from(size), al: 1, t: size, z: 1, n: 1 },
                0, bytes.to_vec(), SymbolRecordFlags::SYSTEMATIC_RUN_START,
            )])
        }
        fn decode(&self, _: &Cx, id: ObjectId, records: &[SymbolRecord]) -> Result<Vec<u8>> {
            let bytes = reconstruct_systematic_happy_path(records)
                .map_err(|error| corrupt(&error.to_string()))?;
            if ObjectId::derive_from_canonical_bytes(&bytes) != id {
                return Err(corrupt("live recovery test object identity mismatch"));
            }
            Ok(bytes)
        }
    }

    type File = <MemoryVfs as Vfs>::File;
    type Driver = DurableWriteCoordinator<File, File, Codec>;

    fn run(future: impl Future<Output = ()>) {
        let _guard = crate::metrics::GLOBAL_GROUP_COMMIT_METRICS_TEST_LOCK.lock().unwrap();
        RuntimeBuilder::current_thread().blocking_threads(1, 2).build().unwrap().block_on(future);
    }

    fn file(vfs: &MemoryVfs, cx: &Cx, name: &str) -> File {
        vfs.open(cx, Some(Path::new(name)),
            VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL).unwrap().0
    }

    fn driver(vfs: &MemoryVfs, cx: &Cx) -> Driver {
        let log = NativeDurabilityLog::create(cx,
            file(vfs, cx, "objects"), file(vfs, cx, "markers"),
            NativeDurabilityLimits::default()).unwrap();
        Driver::new(log, Codec, 8).unwrap()
    }

    async fn queue(driver: &mut Driver, cx: &Cx, value: u8, time: u64) -> CommitSeq {
        let bytes = [value; 8];
        driver.stage_symbols(cx, &Codec.encode(cx, &bytes).unwrap()).await.unwrap();
        let submission = CommitSubmission {
            capsule_object_id: ObjectId::derive_from_canonical_bytes(&bytes),
            capsule_digest: *blake3::hash(&bytes).as_bytes(),
            write_set_pages: vec![PageNumber::new(u32::from(value)).unwrap()],
            witness_refs: vec![], edge_ids: vec![], merge_witness_ids: vec![],
            txn_token: TxnToken::new(TxnId::new(u64::from(value)).unwrap(), TxnEpoch::new(1)),
            begin_seq: driver.committed_tip(),
        };
        driver.queue(cx, submission, time).unwrap()
    }

    fn refused(driver: Driver) -> Driver {
        match driver.try_into_recovery() {
            Err(driver) => driver,
            Ok(_) => panic!("transfer discarded a live obligation"),
        }
    }

    #[test]
    fn live_transfer_preserves_queued_work_and_undelivered_acknowledgements() {
        run(async {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let mut owner = driver(&vfs, &cx);
            let seq = queue(&mut owner, &cx, 1, 100).await;
            owner = refused(owner);
            assert_eq!(owner.pending_count(), 1);
            assert_eq!(owner.committed_tip(), CommitSeq::ZERO);
            assert!(owner.flush(&cx, |_, _| Err::<(), _>(FrankenError::Abort)).await.is_err());
            owner = refused(owner); // A pre-I/O rejection did not abandon the queue.
            owner.flush(&cx, |_, _| Ok(())).await.unwrap();
            owner = refused(owner);
            assert_eq!(owner.take_committed(seq).unwrap().txn_token.id.get(), 1);
            let (mut storage, codec) = owner.try_into_recovery()
                .unwrap_or_else(|_| panic!("quiescent publisher must transfer"));
            storage.prepare(&cx, |id, records| ready(codec.decode(&cx, id, &records).map(|_| ())))
                .await.unwrap();
            let mut replay = NativeCoordinatorRecovery::new(storage.finish().unwrap(), codec, 8);
            replay.prepare(&cx, |_, _| Ok(())).await.unwrap();
            let (mut owner, report) = replay.finish().unwrap();
            assert_eq!(report.markers.len(), 1);
            assert_eq!(owner.pending_count(), 0);
            assert!(owner.take_committed(seq).is_none());
            let next = queue(&mut owner, &cx, 2, 0).await;
            owner.flush(&cx, |_, _| Ok(())).await.unwrap();
            let ack = owner.take_committed(next).unwrap();
            assert_eq!(ack.commit_seq, CommitSeq::new(2));
            assert_eq!(ack.commit_time_unix_ns, 101);
            owner.close(&cx).unwrap();
        });
    }

    #[test]
    fn failed_later_batch_cannot_discard_an_earlier_success_during_transfer() {
        run(async {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let mut owner = driver(&vfs, &cx);
            let first = queue(&mut owner, &cx, 1, 100).await;
            owner.flush(&cx, |_, _| Ok(())).await.unwrap();
            queue(&mut owner, &cx, 2, 101).await;
            // A real source-length error during proof staging, not a synthetic
            // recovery flag. The independently appended bytes are an orphan tail.
            let mut objects = file(&vfs, &cx, "objects");
            let end = objects.file_size(&cx).unwrap();
            objects.write(&cx, b"torn", end).await.unwrap();
            objects.close(&cx).unwrap();
            assert!(owner.flush(&cx, |_, _| Ok(())).await.is_err());
            assert!(owner.needs_recovery());
            owner = refused(owner);
            assert_eq!(owner.take_committed(first).unwrap().commit_seq, first);
            let (mut storage, codec) = owner.try_into_recovery()
                .unwrap_or_else(|_| panic!("failed reservations may transfer after ACK delivery"));
            storage.prepare(&cx, |id, records| ready(codec.decode(&cx, id, &records).map(|_| ())))
                .await.unwrap();
            let recovered = storage.finish().unwrap();
            assert_eq!(recovered.report().markers.len(), 1);
            assert_eq!(recovered.report().symbol_tail_bytes, 4);
            let (mut log, _) = recovered.into_log();
            assert!(log.needs_recovery());
            log.close(&cx).unwrap();
        });
    }

    #[test]
    fn live_transfer_carries_the_acknowledged_prefix_fence_across_retries() {
        run(async {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let mut owner = driver(&vfs, &cx);
            let seq = queue(&mut owner, &cx, 1, 100).await;
            owner.flush(&cx, |_, _| Ok(())).await.unwrap();
            owner.take_committed(seq).unwrap();
            let mut markers = file(&vfs, &cx, "markers");
            let mut original = vec![0; usize::try_from(markers.file_size(&cx).unwrap()).unwrap()];
            assert_eq!(markers.read(&cx, &mut original, 0).await.unwrap(), original.len());
            markers.truncate(&cx, 0).unwrap();
            let (mut storage, codec) = owner.try_into_recovery()
                .unwrap_or_else(|_| panic!("quiescent publisher must transfer"));
            for _ in 0..2 {
                assert!(matches!(storage.prepare(&cx, |id, records| {
                    ready(codec.decode(&cx, id, &records).map(|_| ()))
                }).await, Err(FrankenError::WalCorrupt { .. })));
            }
            markers.write(&cx, &original, 0).await.unwrap();
            markers.close(&cx).unwrap();
            storage.prepare(&cx, |id, records| ready(codec.decode(&cx, id, &records).map(|_| ())))
                .await.unwrap();
            let (mut log, report) = storage.finish().unwrap().into_log();
            assert_eq!(report.markers.len(), 1);
            log.close(&cx).unwrap();
        });
    }

    #[test]
    fn closing_live_publisher_remains_owned_and_cannot_enter_recovery() {
        run(async {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let mut owner = driver(&vfs, &cx);
            owner.close(&cx).unwrap();
            owner = refused(owner);
            owner.close(&cx).unwrap();
        });
    }
}
