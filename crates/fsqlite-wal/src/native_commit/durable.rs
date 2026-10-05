//! Connect native commit admission to storage-backed publication.
//!
//! The in-memory coordinator is private: callers cannot advance its barriers
//! independently of the owned `NativeDurabilityLog`. A final validation hook
//! is mandatory, receives decoded capsules and their immediate evidence, and
//! must bind each write-set summary to its capsule and validate live SSI.
//! This module does not replace that live witness authority or activate SQL
//! native mode. Namespace/append-owner authority must obey the log's contract.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::{CommitMarker, CommitSeq, ObjectId, OperatingMode, SymbolRecord, TxnEpoch, TxnId, TxnToken};
use fsqlite_vfs::{VfsFile, VfsWriteCompletion};

use super::{CommitResult, CommitSubmission, FsyncBarriers, WriteCoordinator};
use crate::metrics::GLOBAL_GROUP_COMMIT_METRICS;
use crate::native_durability::{NativeDurabilityLog, NativeDurabilityReceipt};

/// Maximum bytes in one canonical admission proof.
pub const MAX_NATIVE_PROOF_BYTES: usize = 1024 * 1024;
/// Bounds queued proof buffers and each decoded validation batch independently.
pub const MAX_NATIVE_VALIDATION_BYTES: usize = 16 * 1024 * 1024;
const MAX_PENDING_COMMITS: usize = 1024;
const PROOF_MAGIC: &[u8; 8] = b"FNCP\x01\0\0\0";
const PROOF_FIXED_BYTES: usize = 108;

/// An object encoder/decoder, not a claim of durability. Implementations must
/// verify object identity, envelope integrity, OTI consistency, and required
/// authentication. Decoding must be bounded and must fail on insufficient rank.
/// `encode` must return real source/repair symbols, never fabricated repairs.
/// All methods inherit the caller's cancellation/runtime context.
pub trait NativeObjectCodec {
    /// Encode one nonempty, canonical object payload.
    fn encode(&self, cx: &Cx, payload: &[u8]) -> Result<Vec<SymbolRecord>>;
    /// Reconstruct and verify an object's exact, unpadded canonical payload.
    fn decode(&self, cx: &Cx, object_id: ObjectId, records: &[SymbolRecord]) -> Result<Vec<u8>>;
}

/// Canonical durable admission record. This binds the marker to the complete
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
        let refs = sub.witness_refs.len().checked_add(sub.edge_ids.len())
            .and_then(|n| n.checked_add(sub.merge_witness_ids.len()))
            .ok_or(FrankenError::TooBig)?;
        let len = sub.write_set_pages.len().checked_mul(4)
            .and_then(|n| refs.checked_mul(16).and_then(|r| n.checked_add(r)))
            .and_then(|n| n.checked_add(PROOF_FIXED_BYTES))
            .filter(|n| *n <= MAX_NATIVE_PROOF_BYTES)
            .ok_or(FrankenError::TooBig)?;
        let mut out = Vec::new();
        out.try_reserve_exact(len).map_err(|_| FrankenError::OutOfMemory)?;
        out.extend_from_slice(PROOF_MAGIC);
        out.extend_from_slice(&self.commit_seq.get().to_le_bytes());
        out.extend_from_slice(&self.commit_time_unix_ns.to_le_bytes());
        out.extend_from_slice(sub.capsule_object_id.as_bytes());
        out.extend_from_slice(&sub.capsule_digest);
        out.extend_from_slice(&sub.txn_token.id.get().to_le_bytes());
        out.extend_from_slice(&sub.txn_token.epoch.get().to_le_bytes());
        out.extend_from_slice(&sub.begin_seq.get().to_le_bytes());
        for count in [sub.write_set_pages.len(), sub.witness_refs.len(), sub.edge_ids.len(), sub.merge_witness_ids.len()] {
            out.extend_from_slice(&u32::try_from(count).map_err(|_| FrankenError::TooBig)?.to_le_bytes());
        }
        for page in &sub.write_set_pages {
            out.extend_from_slice(&page.get().to_le_bytes());
        }
        for id in sub.witness_refs.iter().chain(&sub.edge_ids).chain(&sub.merge_witness_ids) {
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
            *count = usize::try_from(u32::from_le_bytes(input.take()?)).map_err(|_| FrankenError::TooBig)?;
        }
        let refs = counts[1].checked_add(counts[2]).and_then(|n| n.checked_add(counts[3]))
            .ok_or(FrankenError::TooBig)?;
        let remaining_len = counts[0].checked_mul(4)
            .and_then(|n| refs.checked_mul(16).and_then(|r| n.checked_add(r)))
            .ok_or(FrankenError::TooBig)?;
        if input.remaining.len() != remaining_len || commit_seq == CommitSeq::ZERO || begin_seq >= commit_seq {
            return Err(corrupt("invalid native proof counts or snapshot"));
        }
        let mut write_set_pages = Vec::new();
        write_set_pages.try_reserve_exact(counts[0]).map_err(|_| FrankenError::OutOfMemory)?;
        for _ in 0..counts[0] {
            write_set_pages.push(fsqlite_types::PageNumber::new(u32::from_le_bytes(input.take()?))
                .ok_or_else(|| corrupt("zero page in native proof"))?);
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

struct ProofReader<'a> { remaining: &'a [u8] }
impl ProofReader<'_> {
    fn take<const N: usize>(&mut self) -> Result<[u8; N]> {
        let bytes = self.remaining.get(..N).ok_or_else(|| corrupt("short native proof field"))?;
        let value = bytes.try_into().map_err(|_| corrupt("short native proof field"))?;
        self.remaining = &self.remaining[N..];
        Ok(value)
    }
    fn object_ids(&mut self, count: usize) -> Result<Vec<ObjectId>> {
        let mut ids = Vec::new();
        ids.try_reserve_exact(count).map_err(|_| FrankenError::OutOfMemory)?;
        for _ in 0..count { ids.push(ObjectId::from_bytes(self.take()?)); }
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
    fn from(error: FrankenError) -> Self { Self::Failure(error) }
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
        match self { Self::Failure(error) => Some(error), Self::Rejected(_) => None }
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

/// Input to final admission. The callback must bind metadata to the decoded
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

/// Owns both the sequencer and its storage path. No mutable accessor exposes
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
        if max_pending == 0 || max_pending > MAX_PENDING_COMMITS {
            return Err(FrankenError::OutOfRange {
                what: "native coordinator max_pending".to_owned(), value: max_pending.to_string(),
            });
        }
        if log.published_tip() != CommitSeq::ZERO || log.needs_recovery() {
            return Err(FrankenError::BusyRecovery);
        }
        Ok(Self {
            coordinator: WriteCoordinator::new(OperatingMode::Native, CommitSeq::ZERO, max_pending),
            log, codec, proofs: BTreeMap::new(), proof_bytes: 0,
            metadata_sizes: BTreeMap::new(), metadata_bytes: 0,
            recovery_required: false, closing: false,
        })
    }

    #[must_use]
    pub const fn committed_tip(&self) -> CommitSeq { self.coordinator.commit_seq_tip }
    #[must_use]
    pub fn pending_count(&self) -> usize { self.coordinator.pending_count() }
    #[must_use]
    pub fn needs_recovery(&self) -> bool { self.recovery_required || self.log.needs_recovery() }
    #[must_use]
    pub fn outstanding_write(&self) -> Option<VfsWriteCompletion> { self.log.outstanding_write() }

    fn ready(&self) -> Result<()> {
        if self.needs_recovery() || self.closing { return Err(FrankenError::BusyRecovery); }
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
        &mut self, cx: &Cx, submission: CommitSubmission, now_unix_ns: u64,
    ) -> std::result::Result<CommitSeq, DurableCommitError> {
        self.ready()?;
        checkpoint(cx)?;
        if submission.begin_seq > self.committed_tip() {
            return Err(corrupt("native submission claims an uncommitted snapshot").into());
        }
        if self.coordinator.batch.is_full() { return Err(FrankenError::Busy.into()); }
        if self.coordinator.batch.pending.iter().any(|pc| pc.submission.txn_token == submission.txn_token) {
            return Err(corrupt("native transaction token is already queued").into());
        }
        self.coordinator.validate(&submission).map_err(DurableCommitError::Rejected)?;
        let seq = self.coordinator.allocated_seq_tip.get().checked_add(1)
            .map(CommitSeq::new).ok_or(FrankenError::DatabaseFull)?;
        let time = now_unix_ns.max(self.coordinator.last_commit_time_ns.saturating_add(1));
        let proof = NativeCommitProof { commit_seq: seq, commit_time_unix_ns: time, submission };
        let bytes = proof.to_bytes()?;
        let records = self.codec.encode(cx, &bytes)?;
        let object_id = records.first().ok_or_else(|| corrupt("proof encoder returned no symbols"))?.object_id;
        let encoded_bytes = records.iter().try_fold(bytes.len(), |total, record| {
            if record.object_id != object_id || record.symbol_data.len() != usize::try_from(record.oti.t).map_err(|_| FrankenError::TooBig)? {
                return Err(corrupt("inconsistent encoded proof symbols"));
            }
            total.checked_add(record.symbol_data.len()).and_then(|n| n.checked_add(76)).ok_or(FrankenError::TooBig)
        })?;
        let buffered = self.proof_bytes.checked_add(encoded_bytes).ok_or(FrankenError::TooBig)?;
        let metadata = self.metadata_bytes.checked_add(bytes.len()).ok_or(FrankenError::TooBig)?;
        if buffered.checked_add(metadata).is_none_or(|n| n > MAX_NATIVE_VALIDATION_BYTES) {
            return Err(FrankenError::TooBig.into());
        }
        if self.codec.decode(cx, object_id, &records)? != bytes {
            return Err(corrupt("proof encoder did not preserve canonical admission bytes").into());
        }
        let reserved = self.coordinator.enqueue_validated(proof.submission, now_unix_ns, object_id);
        debug_assert_eq!(reserved, seq);
        self.metadata_sizes.insert(seq, bytes.len());
        self.metadata_bytes = metadata;
        self.proofs.insert(seq, PreparedProof { bytes, records });
        self.proof_bytes = buffered;
        Ok(seq)
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
    pub async fn flush<V, G>(&mut self, cx: &Cx, validate: V) -> Result<Option<NativeDurabilityReceipt>>
    where
        V: FnOnce(&[NativeCommitCandidate], &BTreeMap<ObjectId, Arc<[u8]>>) -> Result<G>,
    {
        self.ready()?;
        checkpoint(cx)?;
        let mut candidates = Vec::new();
        let mut markers = Vec::new();
        candidates.try_reserve(self.proofs.len()).map_err(|_| FrankenError::OutOfMemory)?;
        markers.try_reserve(self.proofs.len()).map_err(|_| FrankenError::OutOfMemory)?;
        let mut previous = self.coordinator.prev_marker_id;
        let mut objects = BTreeMap::<ObjectId, Arc<[u8]>>::new();
        let mut decoded_bytes = 0_usize;
        for pc in &self.coordinator.batch.pending {
            if pc.barriers.all_complete() { continue; }
            let prepared = self.proofs.get(&pc.allocated_seq)
                .ok_or_else(|| corrupt("queued native proof is missing"))?;
            let proof = NativeCommitProof::from_bytes(&prepared.bytes)?;
            for id in std::iter::once(&pc.submission.capsule_object_id)
                .chain(&pc.submission.witness_refs).chain(&pc.submission.edge_ids)
                .chain(&pc.submission.merge_witness_ids)
            {
                if objects.contains_key(id) { continue; }
                let records = self.log.read_object(cx, *id).await?;
                let payload = self.codec.decode(cx, *id, &records)?;
                decoded_bytes = decoded_bytes.checked_add(payload.len())
                    .filter(|n| *n <= MAX_NATIVE_VALIDATION_BYTES).ok_or(FrankenError::TooBig)?;
                objects.insert(*id, Arc::from(payload));
            }
            let capsule = Arc::clone(objects.get(&pc.submission.capsule_object_id)
                .ok_or_else(|| corrupt("native capsule disappeared during validation"))?);
            if blake3::hash(&capsule).as_bytes() != &pc.submission.capsule_digest {
                return Err(corrupt("native capsule digest does not match submission"));
            }
            let marker = CommitMarker::new(pc.allocated_seq, pc.allocated_time_ns,
                pc.submission.capsule_object_id, pc.proof_object_id, previous);
            previous = Some(ObjectId::derive_from_canonical_bytes(&marker.to_record_bytes()));
            markers.push(marker);
            candidates.push(NativeCommitCandidate { proof, proof_object_id: pc.proof_object_id, capsule });
        }
        let Some(last) = markers.last() else { return Ok(None); };
        let new_tip = last.commit_seq;
        let new_epoch = self.coordinator.epoch.checked_add(1).ok_or(FrankenError::DatabaseFull)?;
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
        let receipt = self.log.publish(cx, &markers, |id, records| {
            let result = (|| {
                let payload = codec.decode(cx, id, &records)?;
                let matches = if let Some(candidate) = candidates.iter().find(|candidate| candidate.proof_object_id == id) {
                    proofs.get(&candidate.proof.commit_seq).is_some_and(|prepared| prepared.bytes == payload)
                } else {
                    objects.get(&id).is_some_and(|expected| expected.as_ref() == payload.as_slice())
                };
                if !matches { return Err(corrupt("native publication object changed after validation")); }
                Ok(())
            })();
            std::future::ready(result)
        }).await?.ok_or_else(|| corrupt("nonempty native batch produced no storage receipt"))?;
        if receipt.first_seq != markers[0].commit_seq || receipt.last_seq != new_tip || receipt.commits != markers.len() {
            return Err(corrupt("native storage receipt does not match queued batch"));
        }
        // No I/O, callback, or fallible allocation separates this receipt from
        // completion. In particular, never call the model's fake barrier path.
        for pc in &mut self.coordinator.batch.pending {
            pc.barriers = FsyncBarriers { fsync1_complete: true, fsync2_complete: true };
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
        let index = self.coordinator.batch.pending.iter()
            .position(|pc| pc.allocated_seq == seq && pc.barriers.all_complete())?;
        let pc = self.coordinator.batch.pending.remove(index)?;
        if let Some(bytes) = self.metadata_sizes.remove(&seq) { self.metadata_bytes -= bytes; }
        Some(DurableCommitAcknowledgement {
            txn_token: pc.submission.txn_token, capsule_object_id: pc.submission.capsule_object_id,
            commit_seq: pc.allocated_seq, commit_time_unix_ns: pc.allocated_time_ns,
        })
    }

    /// Stop new admission while allowing an already queued batch to flush.
    pub fn initiate_shutdown(&mut self) { self.coordinator.initiate_shutdown(); }

    /// Close without silently flushing or discarding a healthy pending batch.
    /// An indeterminate batch may close only after its source-owned write settles.
    ///
    /// # Errors
    /// Returns `Busy` for unflushed work, or propagates the log's close failure.
    pub fn close(&mut self, cx: &Cx) -> Result<()> {
        if !self.needs_recovery() && self.coordinator.batch.pending.iter().any(|pc| !pc.barriers.all_complete()) {
            return Err(FrankenError::Busy);
        }
        self.closing = true;
        self.coordinator.initiate_shutdown();
        self.log.close(cx)
    }
}

fn checkpoint(cx: &Cx) -> Result<()> { cx.checkpoint().map_err(|_| FrankenError::Interrupt) }
fn corrupt(detail: &str) -> FrankenError { FrankenError::WalCorrupt { detail: detail.to_owned() } }
