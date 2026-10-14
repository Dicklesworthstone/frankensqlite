//! Retained semantic replay of a completed native storage recovery.
//!
//! This stage only reads: the sealed input already completed rewrite and both
//! durability barriers. Dropping a replay future keeps the physical handles in
//! this owner, and no partial conflict index is ever installed in a live driver.

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
        let (log, report) = recovered.into_log();
        Self { log, report, codec, max_pending, prepared: None, closing: false }
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
        (self.log.into_recovery(), self.codec)
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
