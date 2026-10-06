//! Connect native commit admission to storage-backed publication.
//!
//! The in-memory coordinator is private: callers cannot advance its barriers
//! independently of the owned `NativeDurabilityLog`. A final validation hook
//! is mandatory, receives decoded capsules and their immediate evidence, and
//! must bind each write-set summary to its capsule and validate live SSI.
//! This module does not replace that live witness authority or activate SQL
//! native mode. Namespace/append-owner authority must obey the log's contract.

#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
pub mod codec;

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt;
use std::sync::Arc;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::{
    CommitMarker, CommitSeq, ObjectId, OperatingMode, SymbolRecord, TxnEpoch, TxnId, TxnToken,
};
use fsqlite_vfs::{VfsFile, VfsWriteCompletion};

use super::{CommitResult, CommitSubmission, FsyncBarriers, WriteCoordinator};
use crate::metrics::GLOBAL_GROUP_COMMIT_METRICS;
use crate::native_durability::{
    NativeDurabilityLimits, NativeDurabilityLog, NativeDurabilityReceipt, NativeDurabilityRecovery,
};

/// Maximum bytes in one canonical admission proof.
pub const MAX_NATIVE_PROOF_BYTES: usize = 1024 * 1024;
/// Bounds queued proof buffers and each decoded validation batch independently.
pub const MAX_NATIVE_VALIDATION_BYTES: usize = 16 * 1024 * 1024;
const MAX_PENDING_COMMITS: usize = 1024;
const PROOF_MAGIC: &[u8; 8] = b"FNCP\x01\0\0\0";
const PROOF_FIXED_BYTES: usize = 108;

/// An object encoder/decoder, not a claim of durability.
///
/// Implementations must verify object identity, envelope integrity, OTI consistency, and required
/// authentication. Decoding must be bounded and must fail on insufficient rank.
/// `encode` must return real source/repair symbols, never fabricated repairs.
/// All methods inherit the caller's cancellation/runtime context.
pub trait NativeObjectCodec {
    /// Encode one nonempty, canonical object payload.
    fn encode(&self, cx: &Cx, payload: &[u8]) -> Result<Vec<SymbolRecord>>;
    /// Reconstruct and verify an object's exact, unpadded canonical payload.
    fn decode(&self, cx: &Cx, object_id: ObjectId, records: &[SymbolRecord]) -> Result<Vec<u8>>;
}

/// Canonical durable admission record.
///
/// This binds the marker to the complete
/// submitted metadata, including FCW pages and every immediate evidence ref.
/// It records what was admitted; independent SSI proof checking still requires
/// resolving and evaluating the referenced witness/edge/merge objects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeCommitProof {
    pub commit_seq: CommitSeq,
    pub commit_time_unix_ns: u64,
    pub submission: CommitSubmission,
}

impl NativeCommitProof {
    /// Versioned little-endian encoding with explicit collection lengths.
    ///
    /// # Errors
    /// Refuses zero/non-forward sequences or a proof larger than the bound.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        if self.commit_seq == CommitSeq::ZERO || self.submission.begin_seq >= self.commit_seq {
            return Err(corrupt("invalid native proof snapshot/commit sequence"));
        }
        let sub = &self.submission;
        let refs = sub
            .witness_refs
            .len()
            .checked_add(sub.edge_ids.len())
            .and_then(|n| n.checked_add(sub.merge_witness_ids.len()))
            .ok_or(FrankenError::TooBig)?;
        let len = sub
            .write_set_pages
            .len()
            .checked_mul(4)
            .and_then(|n| refs.checked_mul(16).and_then(|r| n.checked_add(r)))
            .and_then(|n| n.checked_add(PROOF_FIXED_BYTES))
            .filter(|n| *n <= MAX_NATIVE_PROOF_BYTES)
            .ok_or(FrankenError::TooBig)?;
        let mut out = Vec::new();
        out.try_reserve_exact(len)
            .map_err(|_| FrankenError::OutOfMemory)?;
        out.extend_from_slice(PROOF_MAGIC);
        out.extend_from_slice(&self.commit_seq.get().to_le_bytes());
        out.extend_from_slice(&self.commit_time_unix_ns.to_le_bytes());
        out.extend_from_slice(sub.capsule_object_id.as_bytes());
        out.extend_from_slice(&sub.capsule_digest);
        out.extend_from_slice(&sub.txn_token.id.get().to_le_bytes());
        out.extend_from_slice(&sub.txn_token.epoch.get().to_le_bytes());
        out.extend_from_slice(&sub.begin_seq.get().to_le_bytes());
        for count in [
            sub.write_set_pages.len(),
            sub.witness_refs.len(),
            sub.edge_ids.len(),
            sub.merge_witness_ids.len(),
        ] {
            out.extend_from_slice(
                &u32::try_from(count)
                    .map_err(|_| FrankenError::TooBig)?
                    .to_le_bytes(),
            );
        }
        for page in &sub.write_set_pages {
            out.extend_from_slice(&page.get().to_le_bytes());
        }
        for id in sub
            .witness_refs
            .iter()
            .chain(&sub.edge_ids)
            .chain(&sub.merge_witness_ids)
        {
            out.extend_from_slice(id.as_bytes());
        }
        debug_assert_eq!(out.len(), len);
        Ok(out)
    }

    /// Decode a bounded proof, refusing trailing bytes and invalid identifiers.
    ///
    /// # Errors
    /// Returns an error for malformed, oversized, or noncanonical input.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < PROOF_FIXED_BYTES || bytes.len() > MAX_NATIVE_PROOF_BYTES {
            return Err(corrupt("invalid native proof length"));
        }
        let mut input = ProofReader { remaining: bytes };
        if input.take::<8>()? != *PROOF_MAGIC {
            return Err(corrupt("unsupported native proof version"));
        }
        let commit_seq = CommitSeq::new(u64::from_le_bytes(input.take()?));
        let commit_time_unix_ns = u64::from_le_bytes(input.take()?);
        let capsule_object_id = ObjectId::from_bytes(input.take()?);
        let capsule_digest = input.take()?;
        let id = TxnId::new(u64::from_le_bytes(input.take()?))
            .ok_or_else(|| corrupt("invalid native proof transaction id"))?;
        let epoch = TxnEpoch::new(u32::from_le_bytes(input.take()?));
        let begin_seq = CommitSeq::new(u64::from_le_bytes(input.take()?));
        let mut counts = [0_usize; 4];
        for count in &mut counts {
            *count = usize::try_from(u32::from_le_bytes(input.take()?))
                .map_err(|_| FrankenError::TooBig)?;
        }
        let refs = counts[1]
            .checked_add(counts[2])
            .and_then(|n| n.checked_add(counts[3]))
            .ok_or(FrankenError::TooBig)?;
        let remaining_len = counts[0]
            .checked_mul(4)
            .and_then(|n| refs.checked_mul(16).and_then(|r| n.checked_add(r)))
            .ok_or(FrankenError::TooBig)?;
        if input.remaining.len() != remaining_len
            || commit_seq == CommitSeq::ZERO
            || begin_seq >= commit_seq
        {
            return Err(corrupt("invalid native proof counts or snapshot"));
        }
        let mut write_set_pages = Vec::new();
        write_set_pages
            .try_reserve_exact(counts[0])
            .map_err(|_| FrankenError::OutOfMemory)?;
        for _ in 0..counts[0] {
            write_set_pages.push(
                fsqlite_types::PageNumber::new(u32::from_le_bytes(input.take()?))
                    .ok_or_else(|| corrupt("zero page in native proof"))?,
            );
        }
        let witness_refs = input.object_ids(counts[1])?;
        let edge_ids = input.object_ids(counts[2])?;
        let merge_witness_ids = input.object_ids(counts[3])?;
        Ok(Self {
            commit_seq,
            commit_time_unix_ns,
            submission: CommitSubmission {
                capsule_object_id,
                capsule_digest,
                write_set_pages,
                witness_refs,
                edge_ids,
                merge_witness_ids,
                txn_token: TxnToken::new(id, epoch),
                begin_seq,
            },
        })
    }
}

struct ProofReader<'a> {
    remaining: &'a [u8],
}
impl ProofReader<'_> {
    fn take<const N: usize>(&mut self) -> Result<[u8; N]> {
        let bytes = self
            .remaining
            .get(..N)
            .ok_or_else(|| corrupt("short native proof field"))?;
        let value = bytes
            .try_into()
            .map_err(|_| corrupt("short native proof field"))?;
        self.remaining = &self.remaining[N..];
        Ok(value)
    }
    fn object_ids(&mut self, count: usize) -> Result<Vec<ObjectId>> {
        let mut ids = Vec::new();
        ids.try_reserve_exact(count)
            .map_err(|_| FrankenError::OutOfMemory)?;
        for _ in 0..count {
            ids.push(ObjectId::from_bytes(self.take()?));
        }
        Ok(ids)
    }
}

/// Storage failure and commit rejection are different outcomes. In particular,
/// `Failure` after publication starts is not an assertion of transaction abort.
#[derive(Debug)]
pub enum DurableCommitError {
    Rejected(CommitResult),
    Failure(FrankenError),
}
impl From<FrankenError> for DurableCommitError {
    fn from(error: FrankenError) -> Self {
        Self::Failure(error)
    }
}
impl fmt::Display for DurableCommitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected(result) => write!(f, "native commit rejected: {result:?}"),
            Self::Failure(error) => fmt::Display::fmt(error, f),
        }
    }
}
impl std::error::Error for DurableCommitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Failure(error) => Some(error),
            Self::Rejected(_) => None,
        }
    }
}

/// Identity-preserving result collected only after physical publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableCommitAcknowledgement {
    pub txn_token: TxnToken,
    pub capsule_object_id: ObjectId,
    pub commit_seq: CommitSeq,
    pub commit_time_unix_ns: u64,
}

/// Input to final admission.
///
/// The callback must bind metadata to the decoded
/// capsule and revalidate SSI for the complete ordered batch. The object map
/// passed alongside these candidates contains their immediate evidence bytes.
#[derive(Clone)]
pub struct NativeCommitCandidate {
    pub proof: NativeCommitProof,
    pub proof_object_id: ObjectId,
    pub capsule: Arc<[u8]>,
}

struct PreparedProof {
    bytes: Vec<u8>,
    records: Vec<SymbolRecord>,
}

/// Owns both the sequencer and its storage path.
///
/// No mutable accessor exposes
/// the model's simulated barriers. Completed replies remain in the bounded
/// queue until individually collected, even when a later batch fails.
///
/// A failed/dropped mutating flush latches recovery before its first write.
/// The caller must retain the external append lease until any tracked write
/// settles. Dropping this owner is not a substitute for that lease protocol.
pub struct DurableWriteCoordinator<S: VfsFile, M: VfsFile, C: NativeObjectCodec> {
    coordinator: WriteCoordinator,
    log: NativeDurabilityLog<S, M>,
    codec: C,
    proofs: BTreeMap<CommitSeq, PreparedProof>,
    proof_bytes: usize,
    metadata_sizes: BTreeMap<CommitSeq, usize>,
    metadata_bytes: usize,
    recovery_required: bool,
    closing: bool,
}

impl<S: VfsFile, M: VfsFile, C: NativeObjectCodec> DurableWriteCoordinator<S, M, C> {
    /// Adopt a fresh log (staged symbols are allowed, committed markers are not).
    ///
    /// # Errors
    /// Rejects non-genesis/recovery-blocked storage or invalid queue bounds.
    pub fn new(log: NativeDurabilityLog<S, M>, codec: C, max_pending: usize) -> Result<Self> {
        validate_capacity(max_pending)?;
        if log.published_tip() != CommitSeq::ZERO || log.needs_recovery() {
            return Err(FrankenError::BusyRecovery);
        }
        Ok(Self::from_parts(
            WriteCoordinator::new(OperatingMode::Native, CommitSeq::ZERO, max_pending),
            log,
            codec,
        ))
    }

    fn from_parts(coordinator: WriteCoordinator, log: NativeDurabilityLog<S, M>, codec: C) -> Self {
        Self {
            coordinator,
            log,
            codec,
            proofs: BTreeMap::new(),
            proof_bytes: 0,
            metadata_sizes: BTreeMap::new(),
            metadata_bytes: 0,
            recovery_required: false,
            closing: false,
        }
    }

    /// Recover the sequencer and conflict index from stored, bound proofs.
    ///
    /// The lower log verifies the complete marker prefix and re-synchronizes
    /// referents before markers. For each marker this method additionally
    /// decodes its admission proof, binds sequence/time/capsule identity, checks
    /// the capsule digest, and resolves every immediate evidence reference.
    /// `validate` must bind the write set to capsule semantics and verify the
    /// historical SSI evidence. It is called in commit order, with only one
    /// commit's decoded objects retained at a time. A failed validation never
    /// returns a partly reconstructed driver; it does NOT undo stored commits.
    ///
    /// The returned report describes recovered commits, not new replies to
    /// old callers. The acknowledgement queue starts empty. Retained torn
    /// tails continue to block admission; no file is truncated or replaced.
    /// The caller must settle old writes and hold the log's append-owner and
    /// namespace authority for this entire operation.
    ///
    /// # Errors
    /// Returns storage/codec errors or rejects missing, mismatched or invalid
    /// proofs/evidence. No caller-supplied list can substitute for stored FCW
    /// pages. Parent-directory durability and historical SSI authority remain
    /// explicit caller obligations, not inferred from successful decoding.
    pub async fn recover<V>(
        cx: &Cx,
        symbols: S,
        markers: M,
        limits: NativeDurabilityLimits,
        codec: C,
        max_pending: usize,
        mut validate: V,
    ) -> Result<(Self, NativeDurabilityRecovery)>
    where
        V: FnMut(&NativeCommitCandidate, &BTreeMap<ObjectId, Arc<[u8]>>) -> Result<()>,
    {
        validate_capacity(max_pending)?;
        let (log, report) =
            NativeDurabilityLog::recover(cx, symbols, markers, limits, |id, records| {
                std::future::ready(codec.decode(cx, id, &records).map(|_| ()))
            })
            .await?;
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
                objects
                    .get(&marker.capsule_object_id)
                    .ok_or_else(|| corrupt("native recovery capsule is missing"))?,
            );
            if blake3::hash(&capsule).as_bytes() != &proof.submission.capsule_digest {
                return Err(corrupt("native recovery capsule digest mismatch"));
            }
            let candidate = NativeCommitCandidate {
                proof,
                proof_object_id: marker.proof_object_id,
                capsule,
            };
            validate(&candidate, &objects)?;
            // The lower log established a contiguous genesis prefix. Only
            // bound, semantically validated metadata may restore its FCW map.
            coordinator.commit_index.record_commit(
                &candidate.proof.submission.write_set_pages,
                marker.commit_seq,
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
        Ok((Self::from_parts(coordinator, log, codec), report))
    }

    #[must_use]
    pub const fn committed_tip(&self) -> CommitSeq {
        self.coordinator.commit_seq_tip
    }
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.coordinator.pending_count()
    }
    #[must_use]
    pub fn needs_recovery(&self) -> bool {
        self.recovery_required || self.log.needs_recovery()
    }
    #[must_use]
    pub fn outstanding_write(&self) -> Option<VfsWriteCompletion> {
        self.log.outstanding_write()
    }

    fn ready(&self) -> Result<()> {
        if self.needs_recovery() || self.closing {
            return Err(FrankenError::BusyRecovery);
        }
        Ok(())
    }

    /// Stage writer-produced capsules/evidence without changing commit visibility.
    ///
    /// # Errors
    /// Propagates storage limits, corruption, cancellation and uncertain writes.
    pub async fn stage_symbols(&mut self, cx: &Cx, records: &[SymbolRecord]) -> Result<()> {
        self.ready()?;
        self.log.append_symbols(cx, records).await
    }

    /// Queue a commit and encode its bound proof before reserving a sequence.
    /// This is admission, NOT a successful commit acknowledgement. FCW includes
    /// all earlier pending writes; future snapshots and duplicate queued tokens
    /// are refused. Full capsule/SSI validation occurs at the flush boundary.
    ///
    /// # Errors
    /// Returns typed FCW/shutdown rejection or an encoding/resource/state error.
    pub fn queue(
        &mut self,
        cx: &Cx,
        submission: CommitSubmission,
        now_unix_ns: u64,
    ) -> std::result::Result<CommitSeq, DurableCommitError> {
        let mut sequences = self.queue_batch(cx, vec![submission], now_unix_ns)?;
        Ok(sequences.remove(0))
    }

    /// Admit an ordered group without leaving a partially reserved prefix.
    ///
    /// All proofs are encoded and checked, all queue/byte bounds are enforced,
    /// and FCW is checked against both existing reservations and earlier members
    /// before any sequence, clock, conflict entry or acknowledgement is changed.
    /// On an error every input remains unqueued. Existing queued writers and
    /// completed-but-uncollected replies are untouched. The codec may consume
    /// its own resources; this method cannot undo side effects inside a codec.
    ///
    /// This is NOT an atomic multi-transaction durability record. A later flush
    /// shares two syncs but writes separate commit markers; crash recovery can
    /// find a committed prefix. Success here only returns reserved sequences.
    /// Full capsule/read/SSI validation is still mandatory at the flush boundary.
    ///
    /// # Errors
    /// Returns FCW/shutdown rejection, invalid snapshots/tokens, resource limits,
    /// codec errors or cancellation before changing the coordinator's state.
    pub fn queue_batch(
        &mut self,
        cx: &Cx,
        submissions: Vec<CommitSubmission>,
        now_unix_ns: u64,
    ) -> std::result::Result<Vec<CommitSeq>, DurableCommitError> {
        self.ready()?;
        checkpoint(cx)?;
        if self.pending_count().checked_add(submissions.len()).is_none_or(
            |count| count > self.coordinator.batch.max_batch_size,
        ) {
            return Err(FrankenError::Busy.into());
        }
        let mut prepared = Vec::new();
        let mut sequences = Vec::new();
        let mut tokens = HashSet::new();
        prepared.try_reserve_exact(submissions.len()).map_err(|_| FrankenError::OutOfMemory)?;
        sequences.try_reserve_exact(submissions.len()).map_err(|_| FrankenError::OutOfMemory)?;
        tokens.try_reserve(submissions.len()).map_err(|_| FrankenError::OutOfMemory)?;
        let mut pages = BTreeSet::new();
        let mut seq = self.coordinator.allocated_seq_tip;
        let mut time = self.coordinator.last_commit_time_ns;
        let mut buffered = self.proof_bytes;
        let mut metadata = self.metadata_bytes;
        for submission in submissions {
            checkpoint(cx)?;
            if submission.begin_seq > self.committed_tip() {
                return Err(corrupt("native submission claims an uncommitted snapshot").into());
            }
            if !tokens.insert(submission.txn_token) || self.coordinator.batch.pending.iter()
                .any(|pc| pc.submission.txn_token == submission.txn_token)
            {
                return Err(corrupt("native transaction token is already queued").into());
            }
            self.coordinator.validate(&submission).map_err(DurableCommitError::Rejected)?;
            // Every member's snapshot is <= the already committed tip, so any
            // page reserved by an earlier member necessarily conflicts.
            let conflicts: Vec<_> = submission.write_set_pages.iter()
                .filter(|page| pages.contains(*page)).copied().collect();
            if !conflicts.is_empty() {
                GLOBAL_GROUP_COMMIT_METRICS.record_fcw_conflict();
                return Err(DurableCommitError::Rejected(CommitResult::ConflictFcw {
                    conflicting_pages: conflicts,
                }));
            }
            seq = seq.get().checked_add(1).map(CommitSeq::new)
                .ok_or(FrankenError::DatabaseFull)?;
            time = now_unix_ns.max(time.saturating_add(1));
            let proof = NativeCommitProof { commit_seq: seq, commit_time_unix_ns: time, submission };
            let bytes = proof.to_bytes()?;
            let records = self.codec.encode(cx, &bytes)?;
            let object_id = records.first()
                .ok_or_else(|| corrupt("proof encoder returned no symbols"))?.object_id;
            let encoded_bytes = records.iter().try_fold(bytes.len(), |total, record| {
                if record.object_id != object_id || record.symbol_data.len()
                    != usize::try_from(record.oti.t).map_err(|_| FrankenError::TooBig)?
                {
                    return Err(corrupt("inconsistent encoded proof symbols"));
                }
                total.checked_add(record.symbol_data.len()).and_then(|n| n.checked_add(76))
                    .ok_or(FrankenError::TooBig)
            })?;
            buffered = buffered.checked_add(encoded_bytes).ok_or(FrankenError::TooBig)?;
            metadata = metadata.checked_add(bytes.len()).ok_or(FrankenError::TooBig)?;
            if buffered.checked_add(metadata).is_none_or(|n| n > MAX_NATIVE_VALIDATION_BYTES) {
                return Err(FrankenError::TooBig.into());
            }
            if self.codec.decode(cx, object_id, &records)? != bytes {
                return Err(corrupt("proof encoder did not preserve canonical admission bytes").into());
            }
            pages.extend(proof.submission.write_set_pages.iter().copied());
            sequences.push(seq);
            prepared.push((proof, object_id, PreparedProof { bytes, records }));
        }
        // Reserve fallible publication capacity and observe cancellation one
        // final time. No codec, await or Result-returning work follows this.
        self.coordinator.batch.pending.try_reserve(prepared.len())
            .map_err(|_| FrankenError::OutOfMemory)?;
        self.coordinator.commit_index.entries.try_reserve(pages.len())
            .map_err(|_| FrankenError::OutOfMemory)?;
        checkpoint(cx)?;
        for (proof, object_id, encoded) in prepared {
            let reserved = self.coordinator.enqueue_validated(
                proof.submission, proof.commit_time_unix_ns, object_id,
            );
            debug_assert_eq!(reserved, proof.commit_seq);
            self.metadata_sizes.insert(reserved, encoded.bytes.len());
            self.proofs.insert(reserved, encoded);
        }
        self.metadata_bytes = metadata;
        self.proof_bytes = buffered;
        Ok(sequences)
    }

    /// Execute a whole queued batch through verified capsule/evidence reads,
    /// mandatory final validation, proof staging, and the log's two syncs.
    ///
    /// The returned guard is retained across I/O. The validation implementation
    /// owns its SSI/semantic obligations; an external append-owner lease must
    /// additionally outlive an abandoned tracked write, as required by the log.
    /// Validation errors before writes retain the queue without poisoning it.
    /// Any error/drop after proof staging begins requires storage recovery.
    ///
    /// # Errors
    /// Returns validation, decoding, resource or VFS errors. Never returns a
    /// receipt or advances the committed tip when publication did not succeed.
    pub async fn flush<V, G>(
        &mut self,
        cx: &Cx,
        validate: V,
    ) -> Result<Option<NativeDurabilityReceipt>>
    where
        V: FnOnce(&[NativeCommitCandidate], &BTreeMap<ObjectId, Arc<[u8]>>) -> Result<G>,
    {
        self.ready()?;
        checkpoint(cx)?;
        let mut candidates = Vec::new();
        let mut markers = Vec::new();
        candidates
            .try_reserve(self.proofs.len())
            .map_err(|_| FrankenError::OutOfMemory)?;
        markers
            .try_reserve(self.proofs.len())
            .map_err(|_| FrankenError::OutOfMemory)?;
        let mut previous = self.coordinator.prev_marker_id;
        let mut objects = BTreeMap::<ObjectId, Arc<[u8]>>::new();
        let mut decoded_bytes = 0_usize;
        for pc in &self.coordinator.batch.pending {
            if pc.barriers.all_complete() {
                continue;
            }
            let prepared = self
                .proofs
                .get(&pc.allocated_seq)
                .ok_or_else(|| corrupt("queued native proof is missing"))?;
            let proof = NativeCommitProof::from_bytes(&prepared.bytes)?;
            for id in std::iter::once(&pc.submission.capsule_object_id)
                .chain(&pc.submission.witness_refs)
                .chain(&pc.submission.edge_ids)
                .chain(&pc.submission.merge_witness_ids)
            {
                if objects.contains_key(id) {
                    continue;
                }
                let records = self.log.read_object(cx, *id).await?;
                let payload = self.codec.decode(cx, *id, &records)?;
                decoded_bytes = decoded_bytes
                    .checked_add(payload.len())
                    .filter(|n| *n <= MAX_NATIVE_VALIDATION_BYTES)
                    .ok_or(FrankenError::TooBig)?;
                objects.insert(*id, Arc::from(payload));
            }
            let capsule = Arc::clone(
                objects
                    .get(&pc.submission.capsule_object_id)
                    .ok_or_else(|| corrupt("native capsule disappeared during validation"))?,
            );
            if blake3::hash(&capsule).as_bytes() != &pc.submission.capsule_digest {
                return Err(corrupt("native capsule digest does not match submission"));
            }
            let marker = CommitMarker::new(
                pc.allocated_seq,
                pc.allocated_time_ns,
                pc.submission.capsule_object_id,
                pc.proof_object_id,
                previous,
            );
            previous = Some(ObjectId::derive_from_canonical_bytes(
                &marker.to_record_bytes(),
            ));
            markers.push(marker);
            candidates.push(NativeCommitCandidate {
                proof,
                proof_object_id: pc.proof_object_id,
                capsule,
            });
        }
        let Some(last) = markers.last() else {
            return Ok(None);
        };
        let new_tip = last.commit_seq;
        let new_epoch = self
            .coordinator
            .epoch
            .checked_add(1)
            .ok_or(FrankenError::DatabaseFull)?;
        let _validation_guard = validate(&candidates, &objects)?;
        checkpoint(cx)?;
        // From here even a dropped future must leave admission blocked.
        self.recovery_required = true;
        for prepared in self.proofs.values() {
            self.log.append_symbols(cx, &prepared.records).await?;
        }
        // Witnesses/edges/merge evidence are referents too, not just the two
        // IDs named directly by each marker. Recheck them before FSYNC_1.
        for (id, expected) in &objects {
            let records = self.log.read_object(cx, *id).await?;
            if self.codec.decode(cx, *id, &records)?.as_slice() != expected.as_ref() {
                return Err(corrupt("native evidence changed after final validation"));
            }
        }
        let codec = &self.codec;
        let proofs = &self.proofs;
        let receipt = self
            .log
            .publish(cx, &markers, |id, records| {
                let result = (|| {
                    let payload = codec.decode(cx, id, &records)?;
                    let matches = if let Some(candidate) = candidates
                        .iter()
                        .find(|candidate| candidate.proof_object_id == id)
                    {
                        proofs
                            .get(&candidate.proof.commit_seq)
                            .is_some_and(|prepared| prepared.bytes == payload)
                    } else {
                        objects
                            .get(&id)
                            .is_some_and(|expected| expected.as_ref() == payload.as_slice())
                    };
                    if !matches {
                        return Err(corrupt(
                            "native publication object changed after validation",
                        ));
                    }
                    Ok(())
                })();
                std::future::ready(result)
            })
            .await?
            .ok_or_else(|| corrupt("nonempty native batch produced no storage receipt"))?;
        if receipt.first_seq != markers[0].commit_seq
            || receipt.last_seq != new_tip
            || receipt.commits != markers.len()
        {
            return Err(corrupt(
                "native storage receipt does not match queued batch",
            ));
        }
        // No I/O, callback, or fallible allocation separates this receipt from
        // completion. In particular, never call the model's fake barrier path.
        for pc in &mut self.coordinator.batch.pending {
            pc.barriers = FsyncBarriers {
                fsync1_complete: true,
                fsync2_complete: true,
            };
        }
        self.coordinator.commit_seq_tip = new_tip;
        self.coordinator.prev_marker_id = previous;
        self.coordinator.epoch = new_epoch;
        self.proofs.clear();
        self.proof_bytes = 0;
        self.recovery_required = false;
        GLOBAL_GROUP_COMMIT_METRICS.record_fsync1();
        GLOBAL_GROUP_COMMIT_METRICS.record_fsync2();
        Ok(Some(receipt))
    }

    /// Collect only the named writer's completed reply. Earlier/later replies
    /// are untouched; a failed later batch cannot consume an earlier success.
    #[must_use]
    pub fn take_committed(&mut self, seq: CommitSeq) -> Option<DurableCommitAcknowledgement> {
        let index = self
            .coordinator
            .batch
            .pending
            .iter()
            .position(|pc| pc.allocated_seq == seq && pc.barriers.all_complete())?;
        let pc = self.coordinator.batch.pending.remove(index)?;
        if let Some(bytes) = self.metadata_sizes.remove(&seq) {
            self.metadata_bytes -= bytes;
        }
        Some(DurableCommitAcknowledgement {
            txn_token: pc.submission.txn_token,
            capsule_object_id: pc.submission.capsule_object_id,
            commit_seq: pc.allocated_seq,
            commit_time_unix_ns: pc.allocated_time_ns,
        })
    }

    /// Stop new admission while allowing an already queued batch to flush.
    pub fn initiate_shutdown(&mut self) {
        self.coordinator.initiate_shutdown();
    }

    /// Close without silently flushing or discarding a healthy pending batch.
    /// An indeterminate batch may close only after its source-owned write settles.
    ///
    /// # Errors
    /// Returns `Busy` for unflushed work, or propagates the log's close failure.
    pub fn close(&mut self, cx: &Cx) -> Result<()> {
        if !self.needs_recovery()
            && self
                .coordinator
                .batch
                .pending
                .iter()
                .any(|pc| !pc.barriers.all_complete())
        {
            return Err(FrankenError::Busy);
        }
        self.closing = true;
        self.coordinator.initiate_shutdown();
        self.log.close(cx)
    }
}

fn checkpoint(cx: &Cx) -> Result<()> {
    cx.checkpoint().map_err(|_| FrankenError::Interrupt)
}
fn corrupt(detail: &str) -> FrankenError {
    FrankenError::WalCorrupt {
        detail: detail.to_owned(),
    }
}

fn validate_capacity(max_pending: usize) -> Result<()> {
    if max_pending == 0 || max_pending > MAX_PENDING_COMMITS {
        return Err(FrankenError::OutOfRange {
            what: "native coordinator max_pending".to_owned(),
            value: max_pending.to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod group_admission_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_types::{Oti, PageNumber, SymbolRecordFlags, reconstruct_systematic_happy_path};
    use fsqlite_vfs::{MemoryVfs, Vfs};

    use super::*;
    use crate::test_support::FutureResultTestExt;

    struct Codec {
        calls: AtomicUsize,
        fail_at: usize,
        failure: u8,
    }
    impl NativeObjectCodec for Codec {
        fn encode(&self, cx: &Cx, bytes: &[u8]) -> Result<Vec<SymbolRecord>> {
            let call = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
            if call == self.fail_at {
                match self.failure {
                    1 => return Err(FrankenError::Abort),
                    2 => cx.cancel(),
                    3 => panic!("injected proof encoder unwind"),
                    _ => {}
                }
            }
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
                return Err(corrupt("test object identity mismatch"));
            }
            Ok(bytes)
        }
    }
    type File = <MemoryVfs as Vfs>::File;
    type Driver = DurableWriteCoordinator<File, File, Codec>;

    fn file(vfs: &MemoryVfs, cx: &Cx, name: &str) -> File {
        vfs.open(cx, Some(std::path::Path::new(name)),
            VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL).unwrap().0
    }
    fn driver(cx: &Cx, max: usize, failure: u8) -> Driver {
        let vfs = MemoryVfs::new();
        let log = NativeDurabilityLog::create(cx, file(&vfs, cx, "objects"),
            file(&vfs, cx, "markers"), NativeDurabilityLimits::default()).unwrap();
        let mut driver = Driver::new(log, Codec { calls: AtomicUsize::new(0), fail_at: 2, failure }, max).unwrap();
        let plain = Codec { calls: AtomicUsize::new(0), fail_at: 0, failure: 0 };
        for seed in 1_u8..=8 {
            driver.stage_symbols(cx, &plain.encode(cx, &[seed; 8]).unwrap()).expect("stage capsule");
        }
        driver
    }
    fn submission(seed: u8) -> CommitSubmission {
        CommitSubmission {
            capsule_object_id: ObjectId::derive_from_canonical_bytes(&[seed; 8]),
            capsule_digest: *blake3::hash(&[seed; 8]).as_bytes(),
            write_set_pages: vec![PageNumber::new(u32::from(seed)).unwrap()],
            witness_refs: vec![], edge_ids: vec![], merge_witness_ids: vec![],
            txn_token: TxnToken::new(TxnId::new(u64::from(seed)).unwrap(), TxnEpoch::new(1)),
            begin_seq: CommitSeq::ZERO,
        }
    }
    fn guard() -> std::sync::MutexGuard<'static, ()> {
        crate::metrics::GLOBAL_GROUP_COMMIT_METRICS_TEST_LOCK.lock().unwrap()
    }
    fn assert_unreserved(driver: &Driver) {
        assert_eq!(driver.pending_count(), 0);
        assert_eq!(driver.committed_tip(), CommitSeq::ZERO);
        assert_eq!(driver.coordinator.allocated_seq_tip, CommitSeq::ZERO);
        assert_eq!(driver.coordinator.last_commit_time_ns, 0);
        assert!(driver.coordinator.commit_index.entries.is_empty());
        assert!(driver.proofs.is_empty());
        assert!(driver.metadata_sizes.is_empty());
        assert_eq!(driver.proof_bytes, 0);
        assert_eq!(driver.metadata_bytes, 0);
        assert!(!driver.needs_recovery());
    }

    #[test]
    fn native_group_admission_publishes_in_order_without_consuming_other_replies() {
        let _guard = guard();
        let cx = Cx::new(); let mut driver = driver(&cx, 8, 0);
        let first = driver.queue(&cx, submission(1), 100).unwrap();
        driver.flush(&cx, |_, _| Ok(())).expect("first publication");
        let sequences = driver.queue_batch(&cx, vec![submission(2), submission(3)], 50).unwrap();
        assert_eq!(sequences, vec![CommitSeq::new(2), CommitSeq::new(3)]);
        assert_eq!(driver.committed_tip(), first);
        assert!(driver.take_committed(sequences[0]).is_none());
        let receipt = driver.flush(&cx, |candidates, _| {
            assert_eq!(candidates.len(), 2);
            assert_eq!(candidates[0].proof.commit_time_unix_ns, 101);
            assert_eq!(candidates[1].proof.commit_time_unix_ns, 102);
            Ok(())
        }).expect("group publication").unwrap();
        assert_eq!(receipt.commits, 2);
        assert_eq!(driver.take_committed(sequences[1]).unwrap().txn_token, submission(3).txn_token);
        assert_eq!(driver.take_committed(first).unwrap().txn_token, submission(1).txn_token);
        assert_eq!(driver.take_committed(sequences[0]).unwrap().commit_time_unix_ns, 101);
        assert_eq!(driver.pending_count(), 0);
        driver.close(&cx).unwrap();
    }

    #[test]
    fn native_group_admission_rejects_late_conflicts_tokens_and_future_snapshots_atomically() {
        let _guard = guard();
        for case in 0..3 {
            let cx = Cx::new(); let mut driver = driver(&cx, 8, 0);
            let mut second = submission(2);
            match case {
                0 => second.write_set_pages = submission(1).write_set_pages,
                1 => second.txn_token = submission(1).txn_token,
                _ => second.begin_seq = CommitSeq::new(1),
            }
            assert!(driver.queue_batch(&cx, vec![submission(1), second], 500).is_err());
            assert_unreserved(&driver);
            assert_eq!(driver.queue(&cx, submission(1), 10).unwrap(), CommitSeq::new(1));
            driver.flush(&cx, |_, _| Ok(())).expect("rejected group left no FCW residue");
            assert_eq!(driver.take_committed(CommitSeq::new(1)).unwrap().commit_time_unix_ns, 10);
            driver.close(&cx).unwrap();
        }
    }

    #[test]
    fn native_group_admission_codec_error_cancel_and_unwind_leave_no_reserved_prefix() {
        let _guard = guard();
        for failure in 1..=3 {
            let cx = Cx::new(); let mut driver = driver(&cx, 8, failure);
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                driver.queue_batch(&cx, vec![submission(1), submission(2)], 500)
            }));
            if failure == 3 { assert!(outcome.is_err()); }
            else { assert!(outcome.unwrap().is_err()); }
            assert_unreserved(&driver);
            let fresh_cx = Cx::new();
            assert_eq!(driver.queue(&fresh_cx, submission(1), 10).unwrap(), CommitSeq::new(1));
            driver.flush(&fresh_cx, |_, _| Ok(())).expect("retry after failed encoding");
            driver.take_committed(CommitSeq::new(1)).unwrap();
            driver.close(&fresh_cx).unwrap();
        }
    }

    #[test]
    fn native_group_admission_preserves_existing_queue_when_the_group_cannot_fit() {
        let _guard = guard();
        let cx = Cx::new(); let mut driver = driver(&cx, 2, 0);
        driver.queue(&cx, submission(1), 10).unwrap();
        let before = (driver.proof_bytes, driver.metadata_bytes);
        assert!(matches!(driver.queue_batch(&cx, vec![submission(2), submission(3)], 500),
            Err(DurableCommitError::Failure(FrankenError::Busy))));
        assert_eq!(driver.pending_count(), 1);
        assert_eq!((driver.proof_bytes, driver.metadata_bytes), before);
        assert_eq!(driver.coordinator.allocated_seq_tip, CommitSeq::new(1));
        let mut overlapping = submission(3);
        overlapping.write_set_pages = submission(1).write_set_pages;
        assert!(matches!(driver.queue_batch(&cx, vec![overlapping], 500),
            Err(DurableCommitError::Rejected(CommitResult::ConflictFcw { .. }))));
        driver.queue(&cx, submission(2), 20).unwrap();
        driver.flush(&cx, |_, _| Ok(())).expect("original queue still publishable");
        driver.take_committed(CommitSeq::new(1)).unwrap();
        driver.take_committed(CommitSeq::new(2)).unwrap();
        driver.close(&cx).unwrap();
    }

    #[test]
    fn native_group_admission_size_and_sequence_exhaustion_do_not_allocate_prefixes() {
        let _guard = guard();
        let cx = Cx::new(); let mut driver = driver(&cx, 8, 0);
        let mut oversized = submission(2);
        oversized.witness_refs = vec![ObjectId::from_bytes([9; 16]); 65_536];
        assert!(matches!(driver.queue_batch(&cx, vec![submission(1), oversized], 500),
            Err(DurableCommitError::Failure(FrankenError::TooBig))));
        assert_unreserved(&driver);
        // White-box boundary: exhaustion on member two, before either reserves.
        driver.coordinator.allocated_seq_tip = CommitSeq::new(u64::MAX - 1);
        assert!(matches!(driver.queue_batch(&cx, vec![submission(1), submission(2)], 500),
            Err(DurableCommitError::Failure(FrankenError::DatabaseFull))));
        assert_eq!(driver.coordinator.allocated_seq_tip, CommitSeq::new(u64::MAX - 1));
        assert_eq!(driver.coordinator.last_commit_time_ns, 0);
        assert!(driver.coordinator.commit_index.entries.is_empty());
        assert_eq!(driver.pending_count(), 0);
        driver.close(&cx).unwrap();
    }
}
