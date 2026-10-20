//! Exact, process-local resolution of interrupted native commit attempts.
//!
//! A reserved sequence or a transaction token alone is not an outcome: tokens
//! can be reused after recovery, and a different database can have the same
//! sequence. Tickets bind the original publisher, acknowledged history, and the
//! entire canonical admission proof. Resolution runs only after semantic replay.

use std::sync::Arc;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::{CommitSeq, ObjectId, TxnToken};
use fsqlite_vfs::VfsFile;

use super::super::DurableCommitAcknowledgement;
use super::{
    DurableWriteCoordinator, NativeCommitProof, NativeCoordinatorRecovery, NativeObjectCodec,
    checkpoint, corrupt,
};

/// An accepted attempt, NOT a durable acknowledgement or permission to retry.
///
/// Capture before releasing a failed publisher or its transaction. The ticket
/// retains no file borrow, page image, codec, or transaction pin. Keep the actual
/// storage owner and external namespace/append lease continuously alive through
/// recovery and resolution. The private epoch follows only that live owner's
/// recovery handoff, not a close/reopen or a copy of its files. It rotates when
/// replay installs a new publisher, so even an identical retry with a reused
/// token/sequence cannot resolve an old ticket. Resolve BEFORE `finish`.
/// Tickets deliberately cannot be constructed or deserialized by callers.
#[derive(Debug, Clone)]
pub struct NativeCommitAttempt {
    epoch: Arc<()>,
    acknowledged_tip: CommitSeq,
    acknowledged_marker: Option<ObjectId>,
    proof_object_id: ObjectId,
    proof_digest: [u8; 32],
    expected: DurableCommitAcknowledgement,
}

impl NativeCommitAttempt {
    /// Original transaction identity; not globally unique across databases.
    #[must_use]
    pub const fn txn_token(&self) -> TxnToken {
        self.expected.txn_token
    }

    /// Reserved sequence, not evidence that its marker reached storage.
    #[must_use]
    pub const fn commit_seq(&self) -> CommitSeq {
        self.expected.commit_seq
    }

    /// Exact submitted capsule identity, not an independently supplied page map.
    #[must_use]
    pub const fn capsule_object_id(&self) -> ObjectId {
        self.expected.capsule_object_id
    }
}

/// Outcome at one completed, semantically validated recovery boundary.
///
/// This does not send an old reply, modify a transaction handle, or replay SQL.
/// A new transaction must still revalidate its reads and external side effects.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum NativeCommitOutcome {
    /// The exact proof and marker survived and recovery made them durable.
    Committed(DurableCommitAcknowledgement),
    /// The exact attempt is beyond a complete, tail-free recovered prefix.
    /// This is not inferred from an I/O error or from the absence of a reply.
    NotCommitted,
}

impl<S: VfsFile, M: VfsFile, C: NativeObjectCodec> DurableWriteCoordinator<S, M, C> {
    // The coordinator clock includes unflushed reservations. It describes the
    // published snapshot only when allocation has caught up with publication.
    // Page-store capture must never attach a future reservation's timestamp to
    // an older committed image. This reads owned metadata, not either stream.
    pub(crate) fn published_snapshot_metadata(&self) -> Result<(CommitSeq, Option<u64>)> {
        self.ready()?;
        let sequence = self.coordinator.commit_seq_tip;
        if self.coordinator.allocated_seq_tip != sequence {
            return Err(FrankenError::Busy);
        }
        if self.log.published_tip() != sequence {
            return Err(corrupt("native snapshot coordinator/log tip mismatch"));
        }
        let time = (sequence != CommitSeq::ZERO).then_some(self.coordinator.last_commit_time_ns);
        Ok((sequence, time))
    }

    /// Capture one queued or indeterminate attempt without consuming its reply.
    ///
    /// Query before transfer to recovery, either after `queue` and before
    /// `flush`, or after an errored/dropped flush returns storage ownership.
    /// Completed but uncollected replies remain available to `take_committed`.
    /// `None` means no such reservation is retained here, not "never committed".
    /// A capture failure changes no admission, queue, or physical write state.
    ///
    /// # Errors
    /// Refuses started closes or malformed retained admission metadata.
    /// No payload/metadata I/O, mutation, or sync occurs, even during an outage.
    pub fn commit_attempt(&self, token: TxnToken) -> Result<Option<NativeCommitAttempt>> {
        if self.closing {
            return Err(FrankenError::BusyRecovery);
        }
        let Some(pending) = self
            .coordinator
            .batch
            .pending
            .iter()
            .find(|pending| pending.submission.txn_token == token)
        else {
            return Ok(None);
        };
        // Successful flushes release encoded proofs before replies are taken.
        // Reconstruct that same canonical proof from the retained reservation
        // when necessary; never confuse a collected reply with an absent commit.
        let proof_digest = if let Some(proof) = self.proofs.get(&pending.allocated_seq) {
            *blake3::hash(&proof.bytes).as_bytes()
        } else {
            let proof = NativeCommitProof {
                commit_seq: pending.allocated_seq,
                commit_time_unix_ns: pending.allocated_time_ns,
                submission: pending.submission.clone(),
            };
            *blake3::hash(&proof.to_bytes()?).as_bytes()
        };
        Ok(Some(NativeCommitAttempt {
            epoch: Arc::clone(&self.attempt_epoch),
            acknowledged_tip: self.coordinator.commit_seq_tip,
            acknowledged_marker: self.coordinator.prev_marker_id,
            proof_object_id: pending.proof_object_id,
            proof_digest,
            expected: DurableCommitAcknowledgement {
                txn_token: pending.submission.txn_token,
                capsule_object_id: pending.submission.capsule_object_id,
                commit_seq: pending.allocated_seq,
                commit_time_unix_ns: pending.allocated_time_ns,
            },
        }))
    }
}

impl<S: VfsFile, M: VfsFile, C: NativeObjectCodec> NativeCoordinatorRecovery<S, M, C> {
    /// Resolve an original attempt only after the latest `prepare` succeeded.
    ///
    /// The storage handoff must have completed real recovery and this owner
    /// must have accepted every historical semantic validation. An exact match
    /// is answered even when a later torn tail blocks new admission. Absence
    /// is answered only for a completely tail-free prefix. A different proof
    /// occupying the same sequence is an error, never somebody else's receipt
    /// or an inferred abort. Neither successful lookup nor a dropped lookup
    /// future consumes the ticket, installs a driver, or changes stored bytes.
    ///
    /// # Errors
    /// `BusyRecovery` means replay/close/tail or changed-size uncertainty.
    /// `Abort` means a foreign/retired publisher epoch or mismatched occupied slot.
    /// Also propagates cancellation, lost acknowledged history, decode/auth
    /// failures, and changed proof bytes. No error authorizes a commit retry.
    pub async fn resolve_commit(
        &self,
        cx: &Cx,
        attempt: &NativeCommitAttempt,
    ) -> Result<NativeCommitOutcome> {
        if self.closing || self.prepared.is_none() {
            return Err(FrankenError::BusyRecovery);
        }
        checkpoint(cx)?;
        if !self
            .attempt_epoch
            .as_ref()
            .is_some_and(|epoch| Arc::ptr_eq(epoch, &attempt.epoch))
        {
            return Err(FrankenError::Abort);
        }
        self.log.validate_recovered_lengths(cx)?;
        if attempt.acknowledged_tip == CommitSeq::ZERO {
            if attempt.acknowledged_marker.is_some() {
                return Err(corrupt("invalid native attempt genesis boundary"));
            }
        } else {
            let ordinal = usize::try_from(attempt.acknowledged_tip.get() - 1)
                .map_err(|_| FrankenError::TooBig)?;
            let marker = self
                .report
                .markers
                .get(ordinal)
                .ok_or_else(|| corrupt("native attempt lost acknowledged history"))?;
            if marker.commit_seq != attempt.acknowledged_tip
                || Some(ObjectId::derive_from_canonical_bytes(
                    &marker.to_record_bytes(),
                )) != attempt.acknowledged_marker
            {
                return Err(corrupt("native attempt changed acknowledged history"));
            }
        }
        let ordinal =
            usize::try_from(attempt.commit_seq().get() - 1).map_err(|_| FrankenError::TooBig)?;
        let Some(marker) = self.report.markers.get(ordinal) else {
            if self.report.append_blocked() {
                return Err(FrankenError::BusyRecovery);
            }
            // No await or callback separates the checked, immutable report
            // from this answer; this owner cannot admit a new publisher.
            checkpoint(cx)?;
            return Ok(NativeCommitOutcome::NotCommitted);
        };
        if marker.commit_seq != attempt.commit_seq()
            || marker.commit_time_unix_ns != attempt.expected.commit_time_unix_ns
            || marker.capsule_object_id != attempt.capsule_object_id()
            || marker.proof_object_id != attempt.proof_object_id
        {
            return Err(FrankenError::Abort);
        }
        let records = self.log.read_object(cx, marker.proof_object_id).await?;
        let bytes = self.codec.decode(cx, marker.proof_object_id, &records)?;
        if blake3::hash(&bytes).as_bytes() != &attempt.proof_digest {
            return Err(corrupt("native attempt proof changed after admission"));
        }
        let proof = NativeCommitProof::from_bytes(&bytes)?;
        let acknowledgement = DurableCommitAcknowledgement {
            txn_token: proof.submission.txn_token,
            capsule_object_id: proof.submission.capsule_object_id,
            commit_seq: proof.commit_seq,
            commit_time_unix_ns: proof.commit_time_unix_ns,
        };
        if acknowledgement != attempt.expected {
            return Err(corrupt("native attempt receipt does not match its proof"));
        }
        checkpoint(cx)?;
        self.log.validate_recovered_lengths(cx)?;
        Ok(NativeCommitOutcome::Committed(acknowledgement))
    }
}

#[cfg(test)]
mod tests;
