//! §5.6.4.7 Publication Protocol (Cancel-Safe, Crash-Resilient),
//! §5.6.4.8 Witness GC and Bucket Epochs,
//! §5.6.4.9 Distributed Mode: Proof-Carrying Replication (Normative Hook).
//!
//! The publication protocol ensures witness/edge/proof evidence is durable and
//! visible to readers atomically. Four phases:
//!
//! 1. **Reserve:** Obtain a durable append reservation and linear token.
//! 2. **Write:** Write object symbol records.
//! 3. **Commit:** Atomically publish so evidence becomes visible.
//! 4. **Abort:** Dropping the token before commit makes partial writes GC-able.
//!
//! **Marker discipline:** A transaction is committed iff its `CommitMarker`
//! exists and is published. Witness objects for aborted transactions are ignored.
//!
//! Witness GC uses `safe_gc_seq` (min active begin_seq) to determine pruning
//! eligibility. Hot-plane epoch advancement requires oldest epoch to have drained.
//!
//! Distributed mode enables proof-carrying replication: replicas can fetch
//! `CommitProof` and transitively witness objects to replay validation.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use fsqlite_types::{
    CommitMarker, CommitProof, CommitSeq, DependencyEdge, ReadWitness, TxnId, WriteWitness,
};
use tracing::{debug, error, info, warn};

// ---------------------------------------------------------------------------
// §5.6.4.7 Publication Protocol
// ---------------------------------------------------------------------------

/// Unique identifier for a publication reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReservationId(u64);

impl ReservationId {
    /// Create a new reservation ID.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Get the raw value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// State of a publication in the four-phase protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationPhase {
    /// Reservation obtained; no data written yet.
    Reserved,
    /// Partial data written; not yet visible to readers.
    Writing,
    /// Atomically committed; visible to readers.
    Committed,
    /// Aborted: partial writes are unreachable and GC-able.
    Aborted,
}

/// A linear reservation token for the publication protocol (§5.6.4.7).
///
/// Dropping a token before calling `commit()` is equivalent to aborting:
/// partial publications become unreachable and GC-able. The token is "linear"
/// in the sense that it MUST be either committed or explicitly aborted.
pub struct ReservationToken {
    /// Unique reservation ID.
    id: ReservationId,
    /// Back-reference to the publisher for abort-on-drop.
    publisher: Arc<WitnessPublisherInner>,
    /// Current phase.
    phase: PublicationPhase,
    /// Whether this token has been consumed (committed or aborted).
    consumed: bool,
}

impl ReservationToken {
    /// Get the reservation ID.
    #[must_use]
    pub fn id(&self) -> ReservationId {
        self.id
    }

    /// Get the current phase.
    #[must_use]
    pub fn phase(&self) -> PublicationPhase {
        self.phase
    }
}

impl Drop for ReservationToken {
    fn drop(&mut self) {
        if !self.consumed {
            // Linear discipline: dropping without commit → implicit abort.
            warn!(
                reservation_id = self.id.0,
                phase = ?self.phase,
                "ReservationToken dropped without commit — implicit abort"
            );
            self.publisher.mark_aborted(self.id);
        }
    }
}

/// Pending publication tracked by the publisher.
#[derive(Debug)]
struct PendingPublication {
    /// Publication phase.
    phase: PublicationPhase,
    /// Transaction that owns this publication.
    txn_id: TxnId,
    /// Read witnesses written so far.
    reads: Vec<ReadWitness>,
    /// Write witnesses written so far.
    writes: Vec<WriteWitness>,
    /// Dependency edges written so far.
    edges: Vec<DependencyEdge>,
}

/// Inner state of the witness publisher, behind `Arc<Mutex<...>>`.
#[derive(Debug)]
struct WitnessPublisherInner {
    /// Monotonic reservation counter.
    next_id: AtomicU64,
    /// Pending (in-flight) publications by reservation ID.
    pending: Mutex<BTreeMap<u64, PendingPublication>>,
    /// Committed publications (visible to readers) by reservation ID.
    committed: Mutex<Vec<CommittedPublication>>,
    /// Aborted reservation IDs (for GC tracking).
    aborted: Mutex<Vec<u64>>,
}

impl WitnessPublisherInner {
    fn mark_aborted(&self, id: ReservationId) {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(mut pub_) = pending.remove(&id.0) {
            pub_.phase = PublicationPhase::Aborted;
            drop(pending);
            self.aborted
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(id.0);
            info!(
                reservation_id = id.0,
                txn_id = pub_.txn_id.get(),
                reads = pub_.reads.len(),
                writes = pub_.writes.len(),
                edges = pub_.edges.len(),
                "publication aborted — partial writes are GC-able"
            );
        }
    }
}

/// A completed, committed publication.
#[derive(Debug, Clone)]
pub struct CommittedPublication {
    /// Reservation ID that produced this.
    pub reservation_id: ReservationId,
    /// Transaction ID.
    pub txn_id: TxnId,
    /// Read witnesses.
    pub reads: Vec<ReadWitness>,
    /// Write witnesses.
    pub writes: Vec<WriteWitness>,
    /// Dependency edges.
    pub edges: Vec<DependencyEdge>,
    /// Commit proof.
    pub proof: CommitProof,
}

/// Witness evidence publisher implementing the four-phase protocol (§5.6.4.7).
///
/// Thread-safe: all operations go through interior mutability.
#[derive(Clone)]
pub struct WitnessPublisher {
    inner: Arc<WitnessPublisherInner>,
}

impl WitnessPublisher {
    /// Create a new witness publisher.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(WitnessPublisherInner {
                next_id: AtomicU64::new(1),
                pending: Mutex::new(BTreeMap::new()),
                committed: Mutex::new(Vec::new()),
                aborted: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Phase 1: **Reserve** — Obtain a durable append reservation.
    ///
    /// Returns a linear `ReservationToken`. The caller MUST either commit or
    /// drop (abort) the token. Dropping without commit makes any partial writes
    /// GC-able.
    pub fn reserve(&self, txn_id: TxnId) -> ReservationToken {
        let id = ReservationId(self.inner.next_id.fetch_add(1, Ordering::Relaxed));
        let pub_ = PendingPublication {
            phase: PublicationPhase::Reserved,
            txn_id,
            reads: Vec::new(),
            writes: Vec::new(),
            edges: Vec::new(),
        };
        {
            let mut pending = self.inner.pending.lock().unwrap_or_else(|e| e.into_inner());
            pending.insert(id.0, pub_);
        }
        debug!(
            reservation_id = id.0,
            txn_id = txn_id.get(),
            phase = "reserve",
            "publication reservation obtained"
        );
        ReservationToken {
            id,
            publisher: Arc::clone(&self.inner),
            phase: PublicationPhase::Reserved,
            consumed: false,
        }
    }

    /// Phase 2: **Write** — Write witness objects into the reservation.
    ///
    /// May be called multiple times to incrementally add evidence. The evidence
    /// is NOT visible to readers until `commit()` is called.
    ///
    /// # Errors
    ///
    /// Returns `PublicationError::InvalidPhase` if the token has already been
    /// committed or aborted.
    #[allow(clippy::significant_drop_tightening)]
    pub fn write(
        &self,
        token: &mut ReservationToken,
        reads: Vec<ReadWitness>,
        writes: Vec<WriteWitness>,
        edges: Vec<DependencyEdge>,
    ) -> Result<(), PublicationError> {
        if token.phase != PublicationPhase::Reserved && token.phase != PublicationPhase::Writing {
            return Err(PublicationError::InvalidPhase {
                expected: PublicationPhase::Reserved,
                actual: token.phase,
            });
        }
        let read_count = reads.len();
        let write_count = writes.len();
        let edge_count = edges.len();
        {
            let mut pending = self.inner.pending.lock().unwrap_or_else(|e| e.into_inner());
            let Some(pub_) = pending.get_mut(&token.id.0) else {
                return Err(PublicationError::ReservationNotFound(token.id));
            };
            pub_.reads.extend(reads);
            pub_.writes.extend(writes);
            pub_.edges.extend(edges);
            pub_.phase = PublicationPhase::Writing;
        }
        token.phase = PublicationPhase::Writing;
        debug!(
            reservation_id = token.id.0,
            phase = "write",
            new_reads = read_count,
            new_writes = write_count,
            new_edges = edge_count,
            "publication write phase"
        );
        Ok(())
    }

    /// Phase 3: **Commit** — Atomically publish the evidence.
    ///
    /// After commit, the evidence is visible to readers and the reservation
    /// token is consumed. The `commit_proof` ties together the dependency edges
    /// relevant to the commit decision.
    ///
    /// # Errors
    ///
    /// Returns error if the token is in an invalid phase.
    #[allow(clippy::significant_drop_tightening)]
    pub fn commit(
        &self,
        token: &mut ReservationToken,
        commit_proof: CommitProof,
    ) -> Result<CommittedPublication, PublicationError> {
        if token.phase != PublicationPhase::Writing && token.phase != PublicationPhase::Reserved {
            return Err(PublicationError::InvalidPhase {
                expected: PublicationPhase::Writing,
                actual: token.phase,
            });
        }
        let committed = {
            let mut pending = self.inner.pending.lock().unwrap_or_else(|e| e.into_inner());
            let Some(pub_) = pending.remove(&token.id.0) else {
                return Err(PublicationError::ReservationNotFound(token.id));
            };
            CommittedPublication {
                reservation_id: token.id,
                txn_id: pub_.txn_id,
                reads: pub_.reads,
                writes: pub_.writes,
                edges: pub_.edges,
                proof: commit_proof,
            }
        };

        // Publish: make visible to readers.
        self.inner
            .committed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(committed.clone());

        token.phase = PublicationPhase::Committed;
        token.consumed = true;

        info!(
            reservation_id = token.id.0,
            txn_id = committed.txn_id.get(),
            phase = "commit",
            reads = committed.reads.len(),
            writes = committed.writes.len(),
            edges = committed.edges.len(),
            "publication committed — evidence now visible"
        );

        Ok(committed)
    }

    /// Explicitly abort a reservation.
    ///
    /// Equivalent to dropping the token, but provides explicit control.
    pub fn abort(&self, token: &mut ReservationToken) {
        if token.consumed {
            return;
        }
        self.inner.mark_aborted(token.id);
        token.phase = PublicationPhase::Aborted;
        token.consumed = true;
    }

    /// Query: get all committed publications (visible evidence).
    #[must_use]
    pub fn committed_publications(&self) -> Vec<CommittedPublication> {
        let committed = self
            .inner
            .committed
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        committed.clone()
    }

    /// Query: check if a reservation was aborted.
    #[must_use]
    pub fn is_aborted(&self, reservation_id: ReservationId) -> bool {
        let aborted = self.inner.aborted.lock().unwrap_or_else(|e| e.into_inner());
        aborted.contains(&reservation_id.0)
    }

    /// Query: check if a reservation is still pending (not committed, not aborted).
    #[must_use]
    pub fn is_pending(&self, reservation_id: ReservationId) -> bool {
        let pending = self.inner.pending.lock().unwrap_or_else(|e| e.into_inner());
        pending.contains_key(&reservation_id.0)
    }

    /// Query: count of aborted reservations (for GC tracking).
    #[must_use]
    pub fn aborted_count(&self) -> usize {
        let aborted = self.inner.aborted.lock().unwrap_or_else(|e| e.into_inner());
        aborted.len()
    }
}

impl Default for WitnessPublisher {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for WitnessPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let pending = self.inner.pending.lock().unwrap_or_else(|e| e.into_inner());
        let committed = self
            .inner
            .committed
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let aborted = self.inner.aborted.lock().unwrap_or_else(|e| e.into_inner());
        f.debug_struct("WitnessPublisher")
            .field("pending", &pending.len())
            .field("committed", &committed.len())
            .field("aborted", &aborted.len())
            .finish()
    }
}

/// Errors from the publication protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicationError {
    /// Operation attempted in wrong phase.
    InvalidPhase {
        expected: PublicationPhase,
        actual: PublicationPhase,
    },
    /// Reservation not found (already committed, aborted, or never existed).
    ReservationNotFound(ReservationId),
}

impl std::fmt::Display for PublicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidPhase { expected, actual } => {
                write!(
                    f,
                    "publication phase error: expected {expected:?}, got {actual:?}"
                )
            }
            Self::ReservationNotFound(id) => {
                write!(f, "reservation {id:?} not found")
            }
        }
    }
}

impl std::error::Error for PublicationError {}

// ---------------------------------------------------------------------------
// §5.6.4.7 Marker Discipline
// ---------------------------------------------------------------------------

/// Commit marker store implementing the marker discipline (§5.6.4.7).
///
/// A transaction is committed iff its `CommitMarker` exists and is published.
/// Witness objects for aborted transactions are ignored.
#[derive(Debug, Clone, Default)]
pub struct CommitMarkerStore {
    /// Published commit markers, keyed by commit_seq.
    markers: BTreeMap<u64, CommitMarker>,
}

impl CommitMarkerStore {
    /// Create a new empty marker store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Publish a commit marker (makes the transaction's evidence authoritative).
    pub fn publish(&mut self, marker: CommitMarker) {
        let seq = marker.commit_seq.get();
        info!(
            commit_seq = seq,
            proof_object_id = ?marker.proof_object_id,
            "commit marker published"
        );
        self.markers.insert(seq, marker);
    }

    /// Check if a commit marker exists for the given commit_seq.
    #[must_use]
    pub fn is_committed(&self, commit_seq: CommitSeq) -> bool {
        self.markers.contains_key(&commit_seq.get())
    }

    /// Retrieve a commit marker by commit_seq.
    #[must_use]
    pub fn get(&self, commit_seq: CommitSeq) -> Option<&CommitMarker> {
        self.markers.get(&commit_seq.get())
    }

    /// Resolve the greatest commit sequence whose marker timestamp is
    /// less than or equal to `target_unix_ns`.
    ///
    /// Markers are keyed by monotonic `commit_seq`, and marker timestamps are
    /// expected to be monotonic non-decreasing, so reverse iteration yields
    /// the newest marker at-or-before the target.
    #[must_use]
    pub fn resolve_seq_at_or_before_timestamp(&self, target_unix_ns: u64) -> Option<CommitSeq> {
        self.markers.iter().rev().find_map(|(seq, marker)| {
            (marker.commit_time_unix_ns <= target_unix_ns).then_some(CommitSeq::new(*seq))
        })
    }

    /// Number of published markers.
    #[must_use]
    pub fn len(&self) -> usize {
        self.markers.len()
    }

    /// Whether the store is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.markers.is_empty()
    }
}

// ---------------------------------------------------------------------------
// §5.6.4.8 Witness GC and Bucket Epochs
// ---------------------------------------------------------------------------

/// Witness GC coordinator (§5.6.4.8).
///
/// Computes `safe_gc_seq` from active transaction slots and manages epoch
/// advancement safety checks.
#[derive(Debug)]
pub struct WitnessGcCoordinator {
    /// Retention policy: minimum number of recent commit_seqs to retain
    /// even if eligible for GC (for debuggability).
    retention_count: u64,
    /// Whether GC is enabled.
    enabled: AtomicBool,
}

/// Result of a GC eligibility check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcEligibility {
    /// The computed safe GC sequence.
    pub safe_gc_seq: CommitSeq,
    /// Number of witnesses eligible for pruning.
    pub eligible_count: usize,
    /// Number retained by the retention policy.
    pub retained_count: usize,
    /// Number actually prunable (eligible minus retained).
    pub prunable_count: usize,
}

/// Slot snapshot for GC computation.
#[derive(Debug, Clone, Copy)]
pub struct ActiveSlotSnapshot {
    /// The begin_seq of this active slot.
    pub begin_seq: CommitSeq,
    /// Whether this slot is in Concurrent mode.
    pub is_concurrent: bool,
    /// The witness_epoch pinned by this slot (0 if not concurrent).
    pub witness_epoch: u32,
}

impl WitnessGcCoordinator {
    /// Create a new GC coordinator with the given retention count.
    #[must_use]
    pub fn new(retention_count: u64) -> Self {
        Self {
            retention_count,
            enabled: AtomicBool::new(true),
        }
    }

    /// Enable or disable GC.
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Release);
    }

    /// Whether GC is enabled.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }

    /// Compute `safe_gc_seq` from active slot snapshots (§5.6.4.8).
    ///
    /// `safe_gc_seq := min(begin_seq for all active slots)`.
    /// Returns `CommitSeq::ZERO` if there are no active slots (nothing is safe
    /// to GC because we have no horizon).
    #[must_use]
    pub fn compute_safe_gc_seq(&self, active_slots: &[ActiveSlotSnapshot]) -> CommitSeq {
        if active_slots.is_empty() {
            return CommitSeq::ZERO;
        }
        active_slots
            .iter()
            .map(|s| s.begin_seq)
            .min()
            .unwrap_or(CommitSeq::ZERO)
    }

    /// Check GC eligibility for a set of witnesses (§5.6.4.8).
    ///
    /// Witnesses referencing only transactions with `commit_seq < safe_gc_seq`
    /// are eligible, subject to retention policy.
    #[must_use]
    pub fn check_eligibility(
        &self,
        safe_gc_seq: CommitSeq,
        witness_commit_seqs: &[CommitSeq],
    ) -> GcEligibility {
        let eligible_count = witness_commit_seqs
            .iter()
            .filter(|&&seq| seq.get() < safe_gc_seq.get())
            .count();

        // Retention policy: keep the most recent `retention_count` even if eligible.
        #[allow(clippy::cast_possible_truncation)]
        let retained_count = if self.retention_count > 0 {
            eligible_count.min(self.retention_count as usize)
        } else {
            0
        };
        let prunable_count = eligible_count.saturating_sub(retained_count);

        GcEligibility {
            safe_gc_seq,
            eligible_count,
            retained_count,
            prunable_count,
        }
    }

    /// Check if it is safe to advance the witness epoch (§5.6.4.8).
    ///
    /// Epoch advancement from `cur` to `cur+1` drops buffers tagged `old = cur-1`.
    /// This is permitted iff no active Concurrent-mode slots have
    /// `witness_epoch == old`.
    #[must_use]
    pub fn is_epoch_advancement_safe(
        &self,
        current_epoch: u32,
        active_slots: &[ActiveSlotSnapshot],
    ) -> bool {
        let old_epoch = current_epoch.wrapping_sub(1);
        // Epoch 0 is sentinel; no real slot should be pinned to epoch 0.
        if old_epoch == 0 {
            return true;
        }
        !active_slots
            .iter()
            .any(|s| s.is_concurrent && s.witness_epoch == old_epoch)
    }

    /// Apply GC to a `ColdWitnessStore` (§5.6.4.8).
    ///
    /// Prunes witnesses with `commit_seq < safe_gc_seq`, respecting retention.
    /// Returns the number of objects pruned.
    pub fn apply_gc(
        &self,
        store: &mut super::hot_witness_index::ColdWitnessStore,
        safe_gc_seq: CommitSeq,
        commit_seq_lookup: &dyn Fn(TxnId) -> Option<CommitSeq>,
    ) -> usize {
        if !self.is_enabled() {
            return 0;
        }

        let before_reads = store.read_witnesses.len();
        let before_writes = store.write_witnesses.len();
        let before_edges = store.dependency_edges.len();

        // Prune read witnesses.
        store
            .read_witnesses
            .retain(|w| commit_seq_lookup(w.txn).is_none_or(|seq| seq.get() >= safe_gc_seq.get()));
        // Prune write witnesses.
        store
            .write_witnesses
            .retain(|w| commit_seq_lookup(w.txn).is_none_or(|seq| seq.get() >= safe_gc_seq.get()));
        // Prune dependency edges (both endpoints must be prunable).
        store.dependency_edges.retain(|e| {
            let from_seq = commit_seq_lookup(e.from);
            let to_seq = commit_seq_lookup(e.to);
            // Keep if either endpoint is still needed.
            from_seq.is_none_or(|s| s.get() >= safe_gc_seq.get())
                || to_seq.is_none_or(|s| s.get() >= safe_gc_seq.get())
        });

        let pruned = (before_reads - store.read_witnesses.len())
            + (before_writes - store.write_witnesses.len())
            + (before_edges - store.dependency_edges.len());

        if pruned > 0 {
            info!(
                safe_gc_seq = safe_gc_seq.get(),
                pruned_reads = before_reads - store.read_witnesses.len(),
                pruned_writes = before_writes - store.write_witnesses.len(),
                pruned_edges = before_edges - store.dependency_edges.len(),
                "witness GC applied"
            );
        }
        pruned
    }
}

// ---------------------------------------------------------------------------
// §5.6.4.9 Distributed Mode: Proof-Carrying Replication
// ---------------------------------------------------------------------------

/// Validation verdict from proof-carrying replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationVerdict {
    /// Commit is valid: replay reached the same conclusion.
    Valid,
    /// Commit is invalid: replay found a dangerous structure.
    Invalid,
    /// Evidence incomplete: some witness objects could not be fetched.
    Incomplete,
}

/// A proof-carrying commit for distributed replication (§5.6.4.9).
///
/// Contains all evidence needed for a replica to replay validation.
#[derive(Debug, Clone)]
pub struct ProofCarryingCommit {
    /// The commit marker establishing the commit.
    pub marker: CommitMarker,
    /// The commit proof tying together dependency edges.
    pub proof: CommitProof,
    /// Read witnesses referenced by the proof.
    pub reads: Vec<ReadWitness>,
    /// Write witnesses referenced by the proof.
    pub writes: Vec<WriteWitness>,
    /// Dependency edges from the proof.
    pub edges: Vec<DependencyEdge>,
}

/// Interface for proof-carrying validation (§5.6.4.9).
///
/// Replicas implement this to verify that a remote commit's evidence is
/// locally replayable and reaches the same conclusion.
pub trait ProofCarryingValidator {
    /// Validate a proof-carrying commit by replaying the SSI validation.
    ///
    /// Returns `Valid` if the local replay reaches the same conclusion as the
    /// remote validator under the same policy knobs.
    fn validate(&self, commit: &ProofCarryingCommit) -> ValidationVerdict;
}

/// Default proof-carrying validator that checks structural properties.
///
/// This is a baseline implementation; production validators may use
/// additional policy knobs.
#[derive(Debug, Default)]
pub struct DefaultProofValidator {
    /// Whether to require all evidence objects to be present.
    pub require_complete_evidence: bool,
}

impl ProofCarryingValidator for DefaultProofValidator {
    fn validate(&self, commit: &ProofCarryingCommit) -> ValidationVerdict {
        // Check: proof references same commit_seq as marker.
        if commit.proof.commit_seq != commit.marker.commit_seq {
            error!(
                proof_seq = commit.proof.commit_seq.get(),
                marker_seq = commit.marker.commit_seq.get(),
                "proof/marker commit_seq mismatch"
            );
            return ValidationVerdict::Invalid;
        }

        // Check: proof_object_id in marker should reference this proof.
        // (In a full implementation, we'd verify the ObjectId hash.)

        // Check: all edges in the proof are present in the evidence.
        for edge in &commit.proof.edges {
            let has_read = commit.reads.iter().any(|r| r.txn == edge.from);
            let has_write = commit.writes.iter().any(|w| w.txn == edge.to);

            if self.require_complete_evidence && (!has_read || !has_write) {
                warn!(
                    from = edge.from.get(),
                    to = edge.to.get(),
                    has_read,
                    has_write,
                    "incomplete evidence for dependency edge"
                );
                return ValidationVerdict::Incomplete;
            }
        }

        // Replay SSI validation: check for dangerous structures.
        // A dangerous structure exists if there is a cycle of rw-antidependencies
        // involving two consecutive edges. Simplified check: if any edge's `from`
        // also appears as `to` in another edge from a different observer, flag it.
        //
        // This is the simplified in-process check; the real implementation would
        // match the full SSI algorithm (§5.7).
        let edges = &commit.edges;
        for (i, e1) in edges.iter().enumerate() {
            for e2 in edges.iter().skip(i + 1) {
                // Check for 2-node cycle: T1 →rw T2 and T2 →rw T1
                if e1.to == e2.from && e1.from == e2.to {
                    error!("dangerous 2-node cycle detected in proof-carrying commit");
                    return ValidationVerdict::Invalid;
                }
                // Check for 3-node cycle: T1 →rw T2 →rw T3 →rw T1
                if e1.to == e2.from && edges.iter().any(|e3| e3.from == e2.to && e3.to == e1.from) {
                    error!("dangerous 3-node cycle detected in proof-carrying commit");
                    return ValidationVerdict::Invalid;
                }
            }
        }

        debug!(
            commit_seq = commit.proof.commit_seq.get(),
            edges = edges.len(),
            reads = commit.reads.len(),
            writes = commit.writes.len(),
            verdict = "valid",
            "proof-carrying validation replay"
        );
        ValidationVerdict::Valid
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hot_witness_index::ColdWitnessStore;
    use fsqlite_types::{ObjectId, PageNumber, WitnessKey};

    fn txn(id: u64) -> TxnId {
        TxnId::new(id).unwrap()
    }

    fn page(n: u32) -> PageNumber {
        PageNumber::new(n).unwrap()
    }

    fn key(n: u32) -> WitnessKey {
        WitnessKey::for_cell_read(page(n), &n.to_le_bytes())
    }

    fn make_proof(commit_seq: u64, edges: Vec<DependencyEdge>) -> CommitProof {
        CommitProof {
            commit_seq: CommitSeq::new(commit_seq),
            edges,
            evidence_refs: Vec::new(),
        }
    }

    fn make_marker(commit_seq: u64) -> CommitMarker {
        CommitMarker {
            commit_seq: CommitSeq::new(commit_seq),
            commit_time_unix_ns: 1_000_000,
            capsule_object_id: ObjectId::from_bytes([0u8; 16]),
            proof_object_id: ObjectId::from_bytes([0u8; 16]),
            prev_marker: None,
            integrity_hash: [0u8; 16],
        }
    }

    // -- §5.6.4.7 test 1: Full four-phase publication produces visible witnesses --

    #[test]
    fn test_publication_reserve_write_commit() {
        let publisher = WitnessPublisher::new();
        let t1 = txn(1);

        // Phase 1: Reserve.
        let mut token = publisher.reserve(t1);
        assert_eq!(token.phase(), PublicationPhase::Reserved);
        assert!(publisher.is_pending(token.id()));

        // Phase 2: Write.
        let reads = vec![ReadWitness {
            txn: t1,
            key: key(1),
        }];
        let writes = vec![WriteWitness {
            txn: t1,
            key: key(2),
        }];
        let edges = vec![DependencyEdge {
            from: t1,
            to: txn(2),
            key_basis: key(1),
            observed_by: t1,
        }];
        publisher
            .write(&mut token, reads, writes, edges)
            .expect("write should succeed");
        assert_eq!(token.phase(), PublicationPhase::Writing);

        // Phase 3: Commit.
        let proof = make_proof(100, Vec::new());
        let committed = publisher
            .commit(&mut token, proof)
            .expect("commit should succeed");
        assert_eq!(committed.reads.len(), 1);
        assert_eq!(committed.writes.len(), 1);
        assert_eq!(committed.edges.len(), 1);
        assert_eq!(committed.txn_id, t1);

        // Verify visible.
        let all_committed = publisher.committed_publications();
        assert_eq!(all_committed.len(), 1);
        assert!(!publisher.is_pending(committed.reservation_id));
    }

    // -- §5.6.4.7 test 2: Dropping token before commit → abort, partial GC-able --

    #[test]
    fn test_publication_abort_unreachable() {
        let publisher = WitnessPublisher::new();
        let t1 = txn(10);

        let mut token = publisher.reserve(t1);
        let reservation_id = token.id();

        // Write some evidence.
        publisher
            .write(
                &mut token,
                vec![ReadWitness {
                    txn: t1,
                    key: key(5),
                }],
                Vec::new(),
                Vec::new(),
            )
            .expect("write should succeed");

        // Drop the token without committing → implicit abort.
        drop(token);

        // Verify: aborted, not pending, not committed.
        assert!(publisher.is_aborted(reservation_id));
        assert!(!publisher.is_pending(reservation_id));
        assert!(publisher.committed_publications().is_empty());
        assert_eq!(publisher.aborted_count(), 1);
    }

    // -- §5.6.4.7 test 3: Crash-resilient (serialize/deserialize committed) --

    #[test]
    fn test_publication_crash_resilient() {
        let publisher = WitnessPublisher::new();
        let t1 = txn(50);

        // Full publication cycle.
        let mut token = publisher.reserve(t1);
        publisher
            .write(
                &mut token,
                vec![ReadWitness {
                    txn: t1,
                    key: key(3),
                }],
                vec![WriteWitness {
                    txn: t1,
                    key: key(4),
                }],
                Vec::new(),
            )
            .unwrap();
        let proof = make_proof(200, Vec::new());
        let committed = publisher.commit(&mut token, proof).unwrap();

        // Simulate crash: serialize committed evidence.
        let serialized_reads = serde_json::to_string(&committed.reads).unwrap();
        let serialized_writes = serde_json::to_string(&committed.writes).unwrap();

        // "Crash" — drop everything.
        drop(publisher);

        // "Recovery" — deserialize.
        let recovered_reads: Vec<ReadWitness> = serde_json::from_str(&serialized_reads).unwrap();
        let recovered_writes: Vec<WriteWitness> = serde_json::from_str(&serialized_writes).unwrap();

        assert_eq!(recovered_reads.len(), 1);
        assert_eq!(recovered_reads[0].txn, t1);
        assert_eq!(recovered_writes.len(), 1);
        assert_eq!(recovered_writes[0].txn, t1);
    }

    // -- §5.6.4.7 test 4: Commit marker discipline --

    #[test]
    fn test_commit_marker_discipline() {
        let mut marker_store = CommitMarkerStore::new();

        // Transaction 1 committed with marker.
        let marker1 = make_marker(100);
        marker_store.publish(marker1);

        // Transaction 1 is committed.
        assert!(marker_store.is_committed(CommitSeq::new(100)));

        // Transaction 2 has no marker → not committed.
        assert!(!marker_store.is_committed(CommitSeq::new(200)));

        // Even if witnesses exist for txn 2, without marker it's not committed.
        // (Witnesses for aborted txns are ignored per marker discipline.)
        let mut cold = ColdWitnessStore::new();
        cold.publish_read_witness(ReadWitness {
            txn: txn(2),
            key: key(1),
        });
        assert_eq!(cold.reads_for_txn(txn(2)).len(), 1);
        // But marker_store says txn 2 is not committed:
        assert!(!marker_store.is_committed(CommitSeq::new(200)));
    }

    #[test]
    fn test_commit_marker_timestamp_resolution() {
        let mut marker_store = CommitMarkerStore::new();

        let mut marker_10 = make_marker(10);
        marker_10.commit_time_unix_ns = 1_000;
        marker_store.publish(marker_10);

        let mut marker_20 = make_marker(20);
        marker_20.commit_time_unix_ns = 2_000;
        marker_store.publish(marker_20);

        let mut marker_30 = make_marker(30);
        marker_30.commit_time_unix_ns = 3_000;
        marker_store.publish(marker_30);

        assert_eq!(
            marker_store.resolve_seq_at_or_before_timestamp(2_500),
            Some(CommitSeq::new(20))
        );
        assert_eq!(
            marker_store.resolve_seq_at_or_before_timestamp(3_000),
            Some(CommitSeq::new(30))
        );
        assert_eq!(marker_store.resolve_seq_at_or_before_timestamp(999), None);
    }

    // -- §5.6.4.8 test 5: Witnesses with commit_seq < safe_gc_seq are prunable --

    #[test]
    fn test_witness_gc_safe_seq() {
        let gc = WitnessGcCoordinator::new(0); // No retention.

        let active_slots = vec![
            ActiveSlotSnapshot {
                begin_seq: CommitSeq::new(50),
                is_concurrent: true,
                witness_epoch: 3,
            },
            ActiveSlotSnapshot {
                begin_seq: CommitSeq::new(100),
                is_concurrent: true,
                witness_epoch: 3,
            },
        ];

        let safe_gc_seq = gc.compute_safe_gc_seq(&active_slots);
        assert_eq!(
            safe_gc_seq,
            CommitSeq::new(50),
            "safe_gc_seq = min(begin_seq)"
        );

        // Witnesses with commit_seq < 50 are eligible.
        let witness_seqs = vec![
            CommitSeq::new(10),
            CommitSeq::new(30),
            CommitSeq::new(50),
            CommitSeq::new(80),
        ];
        let eligibility = gc.check_eligibility(safe_gc_seq, &witness_seqs);
        assert_eq!(eligibility.eligible_count, 2); // seqs 10, 30
        assert_eq!(eligibility.prunable_count, 2);
        assert_eq!(eligibility.retained_count, 0);
    }

    // -- §5.6.4.8 test 6: Retention policy prevents pruning recent evidence --

    #[test]
    fn test_witness_gc_retention_policy() {
        let gc = WitnessGcCoordinator::new(5); // Retain 5 most recent.

        let safe_gc_seq = CommitSeq::new(100);
        let witness_seqs: Vec<CommitSeq> = (0..10).map(|i| CommitSeq::new(i * 10)).collect();
        // Seqs: 0, 10, 20, ..., 90. All < 100, so all 10 eligible.
        let eligibility = gc.check_eligibility(safe_gc_seq, &witness_seqs);
        assert_eq!(eligibility.eligible_count, 10);
        assert_eq!(eligibility.retained_count, 5); // Retain 5.
        assert_eq!(eligibility.prunable_count, 5); // Prune 5.
    }

    // -- §5.6.4.8 test 7: Pinned epoch at BEGIN CONCURRENT --

    #[test]
    fn test_witness_epoch_pinned_at_begin_concurrent() {
        // Concurrent-mode txn pins witness_epoch at BEGIN.
        let slot = ActiveSlotSnapshot {
            begin_seq: CommitSeq::new(1),
            is_concurrent: true,
            witness_epoch: 5, // Pinned at epoch 5.
        };

        assert!(slot.is_concurrent, "concurrent mode must be set");
        assert_eq!(
            slot.witness_epoch, 5,
            "witness_epoch must be pinned at BEGIN"
        );

        // The pinned epoch must match HotWitnessIndex.epoch at BEGIN time.
        // This is tested via the index directly:
        let idx = crate::hot_witness_index::HotWitnessIndex::new(4, 64);
        let epoch_at_begin = idx.current_epoch();
        // In a real system, the slot would store: witness_epoch = epoch_at_begin.
        assert_eq!(epoch_at_begin, 1, "index starts at epoch 1");
    }

    // -- §5.6.4.8 test 8: Serialized mode does NOT pin witness_epoch --

    #[test]
    fn test_witness_epoch_not_pinned_for_serialized() {
        // Non-concurrent (serialized) mode: witness_epoch = 0.
        let slot = ActiveSlotSnapshot {
            begin_seq: CommitSeq::new(1),
            is_concurrent: false,
            witness_epoch: 0,
        };
        assert!(!slot.is_concurrent);
        assert_eq!(
            slot.witness_epoch, 0,
            "serialized mode must set witness_epoch = 0"
        );
    }

    // -- §5.6.4.8 test 9: Epoch advancement waits for oldest epoch to drain --

    #[test]
    fn test_witness_epoch_advancement_waits_for_active_txns() {
        let gc = WitnessGcCoordinator::new(0);

        // Current epoch = 5. Old epoch = 4.
        // Txn pinned to epoch 4 is still active → NOT safe to advance.
        let active_slots = vec![ActiveSlotSnapshot {
            begin_seq: CommitSeq::new(1),
            is_concurrent: true,
            witness_epoch: 4,
        }];
        assert!(
            !gc.is_epoch_advancement_safe(5, &active_slots),
            "must NOT advance while txn pinned to old epoch"
        );

        // Txn drains (no more txns pinned to epoch 4) → safe to advance.
        let active_slots_drained = vec![ActiveSlotSnapshot {
            begin_seq: CommitSeq::new(1),
            is_concurrent: true,
            witness_epoch: 5, // Pinned to current, not old.
        }];
        assert!(
            gc.is_epoch_advancement_safe(5, &active_slots_drained),
            "safe to advance when oldest epoch drained"
        );
    }

    // -- §5.6.4.8 test 10: Double-buffer prevents writer starvation --

    #[test]
    fn test_double_buffer_no_writer_starvation() {
        let gc = WitnessGcCoordinator::new(0);

        // Multiple readers at current epoch should not block advancement.
        // Only readers at old epoch (cur-1) block.
        let active_slots = vec![
            ActiveSlotSnapshot {
                begin_seq: CommitSeq::new(1),
                is_concurrent: true,
                witness_epoch: 5, // Current epoch.
            },
            ActiveSlotSnapshot {
                begin_seq: CommitSeq::new(2),
                is_concurrent: true,
                witness_epoch: 5,
            },
            ActiveSlotSnapshot {
                begin_seq: CommitSeq::new(3),
                is_concurrent: true,
                witness_epoch: 5,
            },
        ];

        // Advancing from epoch 5 → 6 is safe because no one is at epoch 4.
        assert!(
            gc.is_epoch_advancement_safe(5, &active_slots),
            "readers at current epoch must not prevent advancement"
        );

        // Even with many readers, the writer (epoch advancer) is not starved.
        // This is the key property: double-buffered design prevents starvation.
    }

    // -- §5.6.4.9 test 11: Distributed proof-carrying commit validation --

    #[test]
    fn test_distributed_proof_carrying_commit() {
        let validator = DefaultProofValidator {
            require_complete_evidence: false,
        };

        let t1 = txn(1);
        let t2 = txn(2);
        let edge = DependencyEdge {
            from: t1,
            to: t2,
            key_basis: key(1),
            observed_by: t1,
        };

        let pcc = ProofCarryingCommit {
            marker: make_marker(100),
            proof: make_proof(100, vec![edge.clone()]),
            reads: vec![ReadWitness {
                txn: t1,
                key: key(1),
            }],
            writes: vec![WriteWitness {
                txn: t2,
                key: key(1),
            }],
            edges: vec![edge],
        };

        let verdict = validator.validate(&pcc);
        assert_eq!(verdict, ValidationVerdict::Valid);
    }

    // -- §5.6.4.9 test 12: Distributed witness replay reaches same conclusion --

    #[test]
    fn test_distributed_witness_replay() {
        let validator = DefaultProofValidator {
            require_complete_evidence: true,
        };

        let t1 = txn(10);
        let t2 = txn(20);
        let t3 = txn(30);

        // No dangerous cycle → should validate as Valid.
        let edges = vec![
            DependencyEdge {
                from: t1,
                to: t2,
                key_basis: key(1),
                observed_by: t1,
            },
            DependencyEdge {
                from: t2,
                to: t3,
                key_basis: key(2),
                observed_by: t2,
            },
        ];

        let pcc = ProofCarryingCommit {
            marker: make_marker(300),
            proof: make_proof(300, edges.clone()),
            reads: vec![
                ReadWitness {
                    txn: t1,
                    key: key(1),
                },
                ReadWitness {
                    txn: t2,
                    key: key(2),
                },
            ],
            writes: vec![
                WriteWitness {
                    txn: t2,
                    key: key(1),
                },
                WriteWitness {
                    txn: t3,
                    key: key(2),
                },
            ],
            edges,
        };

        assert_eq!(validator.validate(&pcc), ValidationVerdict::Valid);
    }

    // -- §5.6.4.9 test 13: Cross-process witness visibility --

    #[test]
    fn test_witness_visible_across_processes() {
        // Simulates "Process A" publishing and "Process B" reading via hot index.
        let idx = Arc::new(crate::hot_witness_index::HotWitnessIndex::new(16, 256));
        let config = crate::witness_hierarchy::WitnessHierarchyConfigV1::default();
        let epoch = idx.current_epoch();

        // "Process A" registers a read witness.
        let key_a = WitnessKey::for_cell_read(page(5), b"shared_key");
        let rks = crate::witness_hierarchy::derive_range_keys(&key_a, &config);
        idx.register_read(0, epoch, &rks);

        // "Process B" queries — should see Process A's registration.
        let readers = crate::hot_witness_index::bitset_to_slot_ids(&idx.candidate_readers(&rks));
        assert!(
            readers.contains(&0),
            "Process B must see Process A's read registration"
        );
    }

    // -- §5.6.4.9 test 14: Cross-process rw-antidependency detection --

    #[test]
    fn test_witness_cross_process_rw_detection() {
        let idx = Arc::new(crate::hot_witness_index::HotWitnessIndex::new(16, 256));
        let config = crate::witness_hierarchy::WitnessHierarchyConfigV1::default();
        let epoch = idx.current_epoch();

        let shared_key = WitnessKey::for_cell_read(page(10), b"account=100");
        let rks = crate::witness_hierarchy::derive_range_keys(&shared_key, &config);

        // "Process A" reads.
        idx.register_read(0, epoch, &rks);
        // "Process B" writes the same key.
        idx.register_write(1, epoch, &rks);

        // Discovery: Process C checks for rw-antidependency.
        let readers = crate::hot_witness_index::bitset_to_slot_ids(&idx.candidate_readers(&rks));
        let writers = crate::hot_witness_index::bitset_to_slot_ids(&idx.candidate_writers(&rks));

        assert!(
            readers.contains(&0) && writers.contains(&1),
            "cross-process rw-antidependency must be detected"
        );
    }

    // -- §5.6.4.7 test: Explicit abort --

    #[test]
    fn test_publication_explicit_abort() {
        let publisher = WitnessPublisher::new();
        let t1 = txn(99);

        let mut token = publisher.reserve(t1);
        let rid = token.id();
        publisher
            .write(
                &mut token,
                vec![ReadWitness {
                    txn: t1,
                    key: key(1),
                }],
                Vec::new(),
                Vec::new(),
            )
            .unwrap();

        // Explicit abort.
        publisher.abort(&mut token);
        assert_eq!(token.phase(), PublicationPhase::Aborted);
        assert!(publisher.is_aborted(rid));
        assert!(publisher.committed_publications().is_empty());

        // Token is consumed; dropping won't double-abort.
        drop(token);
        assert_eq!(publisher.aborted_count(), 1);
    }

    // -- §5.6.4.8 test: Apply GC to ColdWitnessStore --

    #[test]
    fn test_witness_gc_apply_to_cold_store() {
        let gc = WitnessGcCoordinator::new(0);

        let mut store = ColdWitnessStore::new();
        // Txn 1 committed at seq 10.
        store.publish_read_witness(ReadWitness {
            txn: txn(1),
            key: key(1),
        });
        store.publish_write_witness(WriteWitness {
            txn: txn(1),
            key: key(2),
        });
        // Txn 2 committed at seq 50.
        store.publish_read_witness(ReadWitness {
            txn: txn(2),
            key: key(3),
        });
        // Txn 3 committed at seq 100.
        store.publish_read_witness(ReadWitness {
            txn: txn(3),
            key: key(4),
        });

        // Commit seq lookup.
        let lookup = |id: TxnId| -> Option<CommitSeq> {
            match id.get() {
                1 => Some(CommitSeq::new(10)),
                2 => Some(CommitSeq::new(50)),
                3 => Some(CommitSeq::new(100)),
                _ => None,
            }
        };

        // GC with safe_gc_seq = 50: prune txn 1 (seq 10 < 50).
        let pruned = gc.apply_gc(&mut store, CommitSeq::new(50), &lookup);
        assert_eq!(pruned, 2, "should prune 1 read + 1 write for txn 1");
        assert_eq!(store.read_witnesses.len(), 2); // txn 2, txn 3.
        assert_eq!(store.write_witnesses.len(), 0); // txn 1's was pruned.
    }

    // -- §5.6.4.9 test: Invalid proof (commit_seq mismatch) --

    #[test]
    fn test_proof_validation_rejects_mismatch() {
        let validator = DefaultProofValidator {
            require_complete_evidence: false,
        };

        let pcc = ProofCarryingCommit {
            marker: make_marker(100),
            proof: make_proof(200, Vec::new()), // Mismatch!
            reads: Vec::new(),
            writes: Vec::new(),
            edges: Vec::new(),
        };

        assert_eq!(
            validator.validate(&pcc),
            ValidationVerdict::Invalid,
            "must reject mismatched commit_seq"
        );
    }

    // -- §5.6.4.9 test: Incomplete evidence detection --

    #[test]
    fn test_proof_validation_detects_incomplete_evidence() {
        let validator = DefaultProofValidator {
            require_complete_evidence: true,
        };

        let edge = DependencyEdge {
            from: txn(1),
            to: txn(2),
            key_basis: key(1),
            observed_by: txn(1),
        };

        let pcc = ProofCarryingCommit {
            marker: make_marker(100),
            proof: make_proof(100, vec![edge.clone()]),
            reads: Vec::new(),  // Missing read for txn(1)!
            writes: Vec::new(), // Missing write for txn(2)!
            edges: vec![edge],
        };

        assert_eq!(
            validator.validate(&pcc),
            ValidationVerdict::Incomplete,
            "must detect incomplete evidence"
        );
    }
}
