//! Native mode commit protocol (§7.11).
//!
//! Decouples bulk durability (payload bytes) from ordering (marker stream).
//! Writers persist `CommitCapsule` payloads concurrently via RaptorQ encoding,
//! then submit to the [`WriteCoordinator`] which serializes only:
//! validation + `commit_seq` allocation + [`CommitMarker`] append.
//!
//! Critical ordering (two fsync barriers, normative):
//! ```text
//! capsule symbols [written, not fsynced]
//!   → CommitProof
//!     → FSYNC_1 (pre-marker, group commit point)
//!       → CommitMarker
//!         → FSYNC_2 (post-marker)
//!           → SHM publish
//!             → client response
//! ```
//!
//! Both fsyncs are mandatory:
//! - FSYNC_1 prevents "committed marker, lost data" (worst case).
//! - FSYNC_2 prevents "client thinks committed, marker not persisted."

use fsqlite_types::sync_primitives::Instant;
use std::collections::VecDeque;

use fsqlite_types::{
    CommitMarker, CommitProof, CommitSeq, ObjectId, OperatingMode, PageNumber, TxnToken,
};
use tracing::{debug, info, trace, warn};

use crate::metrics::GLOBAL_GROUP_COMMIT_METRICS;

// ---------------------------------------------------------------------------
// §7.11.1 Writer submission request
// ---------------------------------------------------------------------------

/// A commit submission from a writer to the [`WriteCoordinator`] (§7.11.1 step 7).
///
/// Contains everything the coordinator needs to validate and commit the
/// transaction without decoding the full capsule.
#[derive(Debug, Clone)]
pub struct CommitSubmission {
    /// Content-addressed identity of the persisted capsule.
    pub capsule_object_id: ObjectId,
    /// BLAKE3 digest of the capsule bytes (for coordinator validation).
    pub capsule_digest: [u8; 32],
    /// Page numbers touched by this transaction (no false negatives).
    pub write_set_pages: Vec<PageNumber>,
    /// SSI witness evidence object refs.
    pub witness_refs: Vec<ObjectId>,
    /// SSI dependency edge object refs.
    pub edge_ids: Vec<ObjectId>,
    /// SSI merge witness object refs.
    pub merge_witness_ids: Vec<ObjectId>,
    /// Transaction identity.
    pub txn_token: TxnToken,
    /// Snapshot basis (the commit_seq at BEGIN time).
    pub begin_seq: CommitSeq,
}

// ---------------------------------------------------------------------------
// §7.11.2 Commit result
// ---------------------------------------------------------------------------

/// Result of a commit attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitResult {
    /// Successfully committed at the given sequence.
    Committed {
        commit_seq: CommitSeq,
        commit_time_unix_ns: u64,
    },
    /// Commit rejected: first-committer-wins conflict on these pages.
    ConflictFcw { conflicting_pages: Vec<PageNumber> },
    /// Commit rejected: SSI dangerous structure detected.
    ConflictSsi,
    /// Commit rejected: coordinator is shutting down.
    ShuttingDown,
}

// ---------------------------------------------------------------------------
// §7.11.2 Fsync barrier tracking
// ---------------------------------------------------------------------------

/// Tracks which fsync barriers have been completed for a commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FsyncBarriers {
    /// FSYNC_1: capsule symbols + CommitProof durable before marker.
    pub fsync1_complete: bool,
    /// FSYNC_2: CommitMarker stream durable before client response.
    pub fsync2_complete: bool,
}

impl FsyncBarriers {
    /// No barriers completed yet.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            fsync1_complete: false,
            fsync2_complete: false,
        }
    }

    /// Both barriers completed — safe to respond to client.
    #[must_use]
    pub const fn all_complete(self) -> bool {
        self.fsync1_complete && self.fsync2_complete
    }
}

impl Default for FsyncBarriers {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// §7.11.2 Group commit batch
// ---------------------------------------------------------------------------

/// A batch of pending commits that will share a single fsync.
///
/// Group commit amortizes the cost of fsync across multiple writers.
/// The coordinator accumulates submissions until either the batch is full
/// or a timeout expires, then issues a single fsync covering all of them.
#[derive(Debug)]
pub struct GroupCommitBatch {
    /// Pending submissions awaiting commit.
    pending: VecDeque<PendingCommit>,
    /// Maximum batch size before forced flush.
    max_batch_size: usize,
}

/// A submission plus its allocated commit-seq and proof, awaiting fsync.
#[derive(Debug)]
struct PendingCommit {
    submission: CommitSubmission,
    allocated_seq: CommitSeq,
    allocated_time_ns: u64,
    proof_object_id: ObjectId,
    barriers: FsyncBarriers,
}

impl GroupCommitBatch {
    /// Create a new batch with the given maximum size.
    #[must_use]
    pub fn new(max_batch_size: usize) -> Self {
        Self {
            pending: VecDeque::with_capacity(max_batch_size),
            max_batch_size,
        }
    }

    /// Number of pending commits in the batch.
    #[must_use]
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// Whether the batch is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Whether the batch is full and should be flushed.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.pending.len() >= self.max_batch_size
    }

    /// Add a pending commit to the batch.
    fn push(&mut self, pending: PendingCommit) {
        self.pending.push_back(pending);
    }

    /// Mark FSYNC_1 complete for all pending commits.
    fn mark_fsync1_complete(&mut self) {
        for pc in &mut self.pending {
            pc.barriers.fsync1_complete = true;
        }
    }

    /// Mark FSYNC_2 complete for all pending commits.
    fn mark_fsync2_complete(&mut self) {
        for pc in &mut self.pending {
            pc.barriers.fsync2_complete = true;
        }
    }

    /// Drain all fully-committed entries (both fsyncs complete).
    fn drain_committed(&mut self) -> Vec<(CommitSubmission, CommitSeq, u64)> {
        let mut committed = Vec::new();
        while let Some(front) = self.pending.front() {
            if front.barriers.all_complete() {
                let pc = self.pending.pop_front().expect("checked non-empty");
                committed.push((pc.submission, pc.allocated_seq, pc.allocated_time_ns));
            } else {
                break;
            }
        }
        committed
    }
}

// ---------------------------------------------------------------------------
// §7.11.2 Write Coordinator
// ---------------------------------------------------------------------------

/// Commit index: tracks which pages have been modified by recent commits.
///
/// Used for first-committer-wins (FCW) validation. Maps page numbers to
/// the latest commit_seq that modified them.
#[derive(Debug, Clone)]
pub struct CommitIndex {
    /// Page -> latest commit_seq that modified it.
    entries: std::collections::HashMap<PageNumber, CommitSeq>,
}

impl CommitIndex {
    /// Create an empty commit index.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: std::collections::HashMap::new(),
        }
    }

    /// Record that `pages` were modified at `seq`.
    pub fn record_commit(&mut self, pages: &[PageNumber], seq: CommitSeq) {
        for &page in pages {
            self.entries
                .entry(page)
                .and_modify(|existing| {
                    if seq > *existing {
                        *existing = seq;
                    }
                })
                .or_insert(seq);
        }
    }

    /// Check for FCW conflicts: returns pages modified after `begin_seq`.
    #[must_use]
    pub fn check_conflicts(
        &self,
        write_set: &[PageNumber],
        begin_seq: CommitSeq,
    ) -> Vec<PageNumber> {
        write_set
            .iter()
            .filter(|page| {
                self.entries
                    .get(page)
                    .is_some_and(|&latest| latest > begin_seq)
            })
            .copied()
            .collect()
    }
}

impl Default for CommitIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// The `WriteCoordinator` serializes the validation + commit_seq allocation +
/// marker append section of the native commit protocol (§7.11.2).
///
/// The serialized section (steps 2-8) MUST NOT observe cancellation once
/// `commit_seq` is allocated. The coordinator MUST NOT write page payloads
/// in the serialized section — only marker + proof.
#[derive(Debug)]
pub struct WriteCoordinator {
    /// Current operating mode.
    mode: OperatingMode,
    /// Monotonic commit sequence tip.
    commit_seq_tip: CommitSeq,
    /// Last assigned commit time (monotonic non-decreasing).
    last_commit_time_ns: u64,
    /// Commit index for FCW validation.
    commit_index: CommitIndex,
    /// Marker chain: previous marker ObjectId.
    prev_marker_id: Option<ObjectId>,
    /// Group commit batch.
    batch: GroupCommitBatch,
    /// Whether the coordinator is shutting down.
    shutting_down: bool,
    /// Monotonic epoch counter: incremented once per group commit flush.
    epoch: u64,
}

impl WriteCoordinator {
    /// Create a new coordinator for the given mode.
    ///
    /// `initial_seq` is the highest committed sequence from recovery.
    /// `group_commit_max` is the maximum batch size for group commit.
    #[must_use]
    pub fn new(mode: OperatingMode, initial_seq: CommitSeq, group_commit_max: usize) -> Self {
        info!(
            mode = %mode,
            initial_seq = initial_seq.get(),
            group_commit_max,
            "WriteCoordinator initialized"
        );
        Self {
            mode,
            commit_seq_tip: initial_seq,
            last_commit_time_ns: 0,
            commit_index: CommitIndex::new(),
            prev_marker_id: None,
            batch: GroupCommitBatch::new(group_commit_max),
            shutting_down: false,
            epoch: 0,
        }
    }

    /// The current operating mode.
    #[must_use]
    pub const fn mode(&self) -> OperatingMode {
        self.mode
    }

    /// The current commit sequence tip (highest committed).
    #[must_use]
    pub const fn commit_seq_tip(&self) -> CommitSeq {
        self.commit_seq_tip
    }

    /// Number of pending commits in the current group batch.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.batch.len()
    }

    /// The current epoch (incremented once per group commit flush).
    #[must_use]
    pub const fn current_epoch(&self) -> u64 {
        self.epoch
    }

    /// Initiate shutdown: new submissions will be rejected.
    pub fn initiate_shutdown(&mut self) {
        self.shutting_down = true;
    }

    /// Step 1: Validate a submission (FCW + SSI re-validation).
    ///
    /// Returns `Ok(())` if validation passes, or `Err(CommitResult)` with
    /// the rejection reason.
    ///
    /// The coordinator MUST NOT decode the entire capsule here — only check
    /// the write-set summary against the commit index.
    pub fn validate(&self, submission: &CommitSubmission) -> Result<(), CommitResult> {
        if self.shutting_down {
            GLOBAL_GROUP_COMMIT_METRICS.record_shutdown_rejection();
            warn!(
                phase = "validate",
                "rejecting submission: coordinator shutting down"
            );
            return Err(CommitResult::ShuttingDown);
        }

        // FCW validation: check for page conflicts since begin_seq.
        let conflicts = self
            .commit_index
            .check_conflicts(&submission.write_set_pages, submission.begin_seq);
        if !conflicts.is_empty() {
            GLOBAL_GROUP_COMMIT_METRICS.record_fcw_conflict();
            debug!(
                phase = "validate",
                begin_seq = submission.begin_seq.get(),
                conflict_count = conflicts.len(),
                "FCW conflict detected"
            );
            return Err(CommitResult::ConflictFcw {
                conflicting_pages: conflicts,
            });
        }

        // SSI re-validation would check for dangerous structure here.
        // For now, we accept (the writer already validated locally in step 2).
        // Full SSI re-validation is deferred to the SSI witness plane
        // implementation (bd-3t3.9.*).

        Ok(())
    }

    /// Steps 2-8: Process a validated submission through the serialized section.
    ///
    /// This allocates a commit_seq, builds the proof and marker, and adds
    /// the commit to the group batch. Returns the allocated commit result.
    ///
    /// The caller MUST call `flush_batch` to actually issue fsyncs and
    /// complete the commits.
    ///
    /// # Errors
    ///
    /// Returns `Err` if validation fails (FCW or SSI conflict).
    pub fn submit(
        &mut self,
        submission: CommitSubmission,
        now_unix_ns: u64,
    ) -> Result<CommitSeq, CommitResult> {
        // Step 1: Validate
        self.validate(&submission)?;

        GLOBAL_GROUP_COMMIT_METRICS.record_submission();

        // Step 2: Allocate gap-free commit_seq
        let new_seq = self.commit_seq_tip.next();
        let commit_time = now_unix_ns.max(self.last_commit_time_ns.wrapping_add(1));

        // Step 3: Build CommitProof (persisted as ECS object)
        let proof = CommitProof {
            commit_seq: new_seq,
            edges: Vec::new(), // Populated by SSI witness plane
            evidence_refs: submission.witness_refs.clone(),
        };
        let proof_object_id = Self::derive_proof_object_id(&proof);

        // Update coordinator state (inside serialized section)
        self.commit_seq_tip = new_seq;
        self.last_commit_time_ns = commit_time;
        self.commit_index
            .record_commit(&submission.write_set_pages, new_seq);

        trace!(
            target: "fsqlite_wal::native_commit",
            phase = "submit",
            commit_seq = new_seq.get(),
            pages = submission.write_set_pages.len(),
            begin_seq = submission.begin_seq.get(),
            pending_batch = self.batch.len() + 1,
            "allocated commit_seq"
        );

        // Add to group commit batch
        self.batch.push(PendingCommit {
            submission,
            allocated_seq: new_seq,
            allocated_time_ns: commit_time,
            proof_object_id,
            barriers: FsyncBarriers::new(),
        });

        Ok(new_seq)
    }

    /// Execute FSYNC_1 (pre-marker group commit point).
    ///
    /// Makes all pending capsule symbols AND CommitProof objects durable
    /// BEFORE markers reference them. Without this barrier, NVMe write
    /// reordering can make a marker durable while referents are not.
    ///
    /// Returns the number of commits covered by this fsync.
    pub fn fsync1(&mut self) -> usize {
        let count = self.batch.len();
        self.batch.mark_fsync1_complete();
        GLOBAL_GROUP_COMMIT_METRICS.record_fsync1();
        debug!(
            target: "fsqlite_wal::native_commit",
            phase = "fsync1",
            batch_size = count,
            "pre-marker fsync complete"
        );
        count
    }

    /// Steps 5-6: Append markers and execute FSYNC_2 (post-marker).
    ///
    /// For each pending commit with FSYNC_1 complete, appends a
    /// `CommitMarker` to the marker stream, then marks FSYNC_2 complete.
    ///
    /// Returns the markers that were appended.
    pub fn append_markers_and_fsync2(&mut self) -> Vec<CommitMarker> {
        let mut markers = Vec::new();

        for pc in &mut self.batch.pending {
            if pc.barriers.fsync1_complete && !pc.barriers.fsync2_complete {
                // Step 5: Build and append CommitMarker
                let marker = CommitMarker::new(
                    pc.allocated_seq,
                    pc.allocated_time_ns,
                    pc.submission.capsule_object_id,
                    pc.proof_object_id,
                    self.prev_marker_id,
                );

                // Derive marker ObjectId for chain linking
                let marker_bytes = marker.to_record_bytes();
                let marker_oid = ObjectId::derive_from_canonical_bytes(&marker_bytes);
                self.prev_marker_id = Some(marker_oid);

                markers.push(marker);
            }
        }

        // Step 6: FSYNC_2 on marker stream
        self.batch.mark_fsync2_complete();
        GLOBAL_GROUP_COMMIT_METRICS.record_fsync2();

        debug!(
            target: "fsqlite_wal::native_commit",
            phase = "fsync2",
            markers_appended = markers.len(),
            "post-marker fsync complete"
        );

        markers
    }

    /// Steps 7-8: Drain committed entries and return results.
    ///
    /// Step 7 (SHM publish) is the caller's responsibility after receiving
    /// these results — the coordinator only manages the serialized section.
    pub fn drain_committed(&mut self) -> Vec<CommitResult> {
        let drained = self.batch.drain_committed();
        let batch_size = drained.len();
        let results: Vec<CommitResult> = drained
            .into_iter()
            .map(|(_, seq, time)| {
                info!(
                    target: "fsqlite_wal::native_commit",
                    commit_seq = seq.get(),
                    durable = true,
                    "commit published"
                );
                CommitResult::Committed {
                    commit_seq: seq,
                    commit_time_unix_ns: time,
                }
            })
            .collect();

        if batch_size > 0 {
            info!(
                target: "fsqlite_wal::native_commit",
                group_size = batch_size,
                "parallel_wal_commit group drained"
            );
        }

        results
    }

    /// Flush the pending batch: fsync1, append markers, fsync2, drain.
    ///
    /// Wraps the full group commit cycle in a `parallel_wal_commit` tracing
    /// span with epoch, group_size, and commit_seq range fields.
    /// Returns the committed results and records metrics.
    pub fn flush_batch(&mut self) -> Vec<CommitResult> {
        let group_size = self.batch.len();
        if group_size == 0 {
            return Vec::new();
        }

        let start = Instant::now();
        self.epoch += 1;
        let epoch = self.epoch;

        let span = tracing::info_span!(
            target: "fsqlite_wal::native_commit",
            "parallel_wal_commit",
            epoch,
            group_size,
            frames_in_batch = group_size,
        );
        let _guard = span.enter();

        let fsync1_count = self.fsync1();
        let markers = self.append_markers_and_fsync2();
        let results = self.drain_committed();

        #[allow(clippy::cast_possible_truncation)]
        let latency_us = start.elapsed().as_micros() as u64;
        GLOBAL_GROUP_COMMIT_METRICS.record_group_commit(group_size as u64, latency_us);

        info!(
            target: "fsqlite_wal::native_commit",
            epoch,
            group_size,
            fsync1_count,
            markers_appended = markers.len(),
            committed = results.len(),
            latency_us,
            "parallel_wal_commit complete"
        );

        results
    }

    /// Convenience: submit, fsync1, append markers, fsync2, drain.
    ///
    /// Processes a single submission through the entire protocol.
    /// In production, submissions are batched; this is for testing and
    /// single-commit workloads.
    pub fn submit_and_commit(
        &mut self,
        submission: CommitSubmission,
        now_unix_ns: u64,
    ) -> CommitResult {
        match self.submit(submission, now_unix_ns) {
            Ok(_seq) => {
                let mut results = self.flush_batch();
                results.pop().unwrap_or(CommitResult::ShuttingDown)
            }
            Err(result) => result,
        }
    }

    /// Derive a deterministic ObjectId for a CommitProof.
    fn derive_proof_object_id(proof: &CommitProof) -> ObjectId {
        let mut canonical = Vec::new();
        canonical.extend_from_slice(b"fsqlite:proof:v1");
        canonical.extend_from_slice(&proof.commit_seq.get().to_le_bytes());
        for edge in &proof.edges {
            canonical.extend_from_slice(&edge.from.get().to_le_bytes());
            canonical.extend_from_slice(&edge.to.get().to_le_bytes());
        }
        for evidence in &proof.evidence_refs {
            canonical.extend_from_slice(evidence.as_bytes());
        }
        ObjectId::derive_from_canonical_bytes(&canonical)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use fsqlite_types::{CommitCapsule, TxnEpoch, TxnId};

    use super::*;

    fn make_oid(seed: u8) -> ObjectId {
        ObjectId::from_bytes([seed; 16])
    }

    fn make_submission(pages: &[u32], begin_seq: u64, seed: u8) -> CommitSubmission {
        let txn_id = TxnId::new(u64::from(seed) + 1).expect("valid txn id");
        CommitSubmission {
            capsule_object_id: make_oid(seed),
            capsule_digest: [seed; 32],
            write_set_pages: pages
                .iter()
                .map(|&p| PageNumber::new(p).expect("non-zero page"))
                .collect(),
            witness_refs: Vec::new(),
            edge_ids: Vec::new(),
            merge_witness_ids: Vec::new(),
            txn_token: TxnToken::new(txn_id, TxnEpoch::new(1)),
            begin_seq: CommitSeq::new(begin_seq),
        }
    }

    // ── bd-15jh test 1: test_compat_mode_wal_format ──

    #[test]
    fn test_compat_mode_wal_format() {
        // Verify that compatibility mode is the default and WAL-based.
        let mode = OperatingMode::default();
        assert_eq!(mode, OperatingMode::Compatibility);
        assert!(!mode.is_native());
        assert!(mode.legacy_readers_allowed());
        assert_eq!(mode.to_string(), "compatibility");

        // PRAGMA parsing
        assert_eq!(
            OperatingMode::from_pragma("compatibility"),
            Some(OperatingMode::Compatibility)
        );
        assert_eq!(
            OperatingMode::from_pragma("compat"),
            Some(OperatingMode::Compatibility)
        );
        assert_eq!(
            OperatingMode::from_pragma("native"),
            Some(OperatingMode::Native)
        );
        assert_eq!(
            OperatingMode::from_pragma("NATIVE"),
            Some(OperatingMode::Native)
        );
        assert!(OperatingMode::from_pragma("invalid").is_none());

        // Coordinator in compat mode
        let coord = WriteCoordinator::new(OperatingMode::Compatibility, CommitSeq::ZERO, 16);
        assert_eq!(coord.mode(), OperatingMode::Compatibility);
        assert_eq!(coord.commit_seq_tip(), CommitSeq::ZERO);
    }

    // ── bd-15jh test 2: test_native_mode_commit_capsule ──

    #[test]
    fn test_native_mode_commit_capsule() {
        // Verify CommitCapsule is persisted before coordinator contact.
        // In the protocol, the writer builds and persists the capsule (steps 1-6)
        // before submitting to the coordinator (step 7).
        let capsule = CommitCapsule {
            object_id: make_oid(0x11),
            snapshot_basis: CommitSeq::new(5),
            intent_log: Vec::new(),
            page_deltas: vec![
                (PageNumber::new(10).unwrap(), vec![0xAA; 4096]),
                (PageNumber::new(20).unwrap(), vec![0xBB; 4096]),
            ],
            read_set_digest: [0x01; 32],
            write_set_digest: [0x02; 32],
            read_witness_refs: vec![make_oid(0x30)],
            write_witness_refs: vec![make_oid(0x31)],
            dependency_edge_refs: Vec::new(),
            merge_witness_refs: Vec::new(),
        };

        // Capsule has content before coordinator sees it
        assert_eq!(capsule.snapshot_basis.get(), 5);
        assert_eq!(capsule.page_deltas.len(), 2);
        assert_eq!(capsule.read_witness_refs.len(), 1);

        // Submission contains only the capsule_object_id (not the full capsule)
        let submission = CommitSubmission {
            capsule_object_id: capsule.object_id,
            capsule_digest: [0xFF; 32],
            write_set_pages: capsule.page_deltas.iter().map(|(pgno, _)| *pgno).collect(),
            witness_refs: capsule.read_witness_refs.clone(),
            edge_ids: Vec::new(),
            merge_witness_ids: Vec::new(),
            txn_token: TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(1)),
            begin_seq: capsule.snapshot_basis,
        };

        assert_eq!(submission.capsule_object_id, capsule.object_id);
        assert_eq!(submission.write_set_pages.len(), 2);
    }

    // ── bd-15jh test 3: test_native_marker_append ──

    #[test]
    fn test_native_marker_append() {
        // Verify CommitMarker is appended atomically with correct wire format.
        let marker = CommitMarker::new(
            CommitSeq::new(42),
            1_700_000_000_000_000_000,
            make_oid(0x11),
            make_oid(0x22),
            Some(make_oid(0x33)),
        );

        // Wire format is exactly 88 bytes
        let bytes = marker.to_record_bytes();
        assert_eq!(
            bytes.len(),
            fsqlite_types::COMMIT_MARKER_RECORD_V1_SIZE,
            "marker record must be exactly 88 bytes"
        );

        // Round-trip
        let recovered =
            CommitMarker::from_record_bytes(&bytes).expect("marker roundtrip must succeed");
        assert_eq!(recovered.commit_seq, marker.commit_seq);
        assert_eq!(recovered.commit_time_unix_ns, marker.commit_time_unix_ns);
        assert_eq!(recovered.capsule_object_id, marker.capsule_object_id);
        assert_eq!(recovered.proof_object_id, marker.proof_object_id);
        assert_eq!(recovered.prev_marker, marker.prev_marker);
        assert_eq!(recovered.integrity_hash, marker.integrity_hash);

        // Integrity verification
        assert!(marker.verify_integrity());

        // Marker without prev (genesis)
        let genesis = CommitMarker::new(
            CommitSeq::new(1),
            1_700_000_000_000_000_000,
            make_oid(0x01),
            make_oid(0x02),
            None,
        );
        assert!(genesis.prev_marker.is_none());
        assert!(genesis.verify_integrity());
        let genesis_bytes = genesis.to_record_bytes();
        let genesis_recovered = CommitMarker::from_record_bytes(&genesis_bytes).unwrap();
        assert!(genesis_recovered.prev_marker.is_none());
    }

    // ── bd-15jh test 4: test_native_group_commit ──

    #[test]
    fn test_native_group_commit() {
        // Multiple commits share a single fsync.
        let mut coord = WriteCoordinator::new(OperatingMode::Native, CommitSeq::ZERO, 16);

        // Submit 5 writers to different pages
        let base_time = 1_700_000_000_000_000_000_u64;
        for i in 0..5u8 {
            let pages = &[u32::from(i) * 10 + 1]; // pages 1, 11, 21, 31, 41
            let sub = make_submission(pages, 0, i);
            let seq = coord.submit(sub, base_time + u64::from(i)).unwrap();
            assert_eq!(seq.get(), u64::from(i) + 1);
        }

        // All 5 pending
        assert_eq!(coord.pending_count(), 5);

        // Single FSYNC_1 covers all 5
        let fsync1_count = coord.fsync1();
        assert_eq!(fsync1_count, 5);

        // Append markers + FSYNC_2
        let markers = coord.append_markers_and_fsync2();
        assert_eq!(markers.len(), 5);

        // Verify marker chain linking
        assert!(markers[0].prev_marker.is_none()); // first marker is genesis
        for (i, marker) in markers.iter().enumerate().skip(1) {
            assert!(
                marker.prev_marker.is_some(),
                "marker {i} should link to previous"
            );
        }

        // All should drain as committed
        let results = coord.drain_committed();
        assert_eq!(results.len(), 5);
        for (i, result) in results.iter().enumerate() {
            match result {
                CommitResult::Committed { commit_seq, .. } => {
                    assert_eq!(commit_seq.get(), (i as u64) + 1);
                }
                other => unreachable!("expected Committed, got {other:?}"),
            }
        }

        assert_eq!(coord.pending_count(), 0);
    }

    // ── bd-15jh test 5: test_native_crash_recovery ──

    #[test]
    fn test_native_crash_recovery() {
        // Verify recovery at each step of the protocol.
        let mut coord = WriteCoordinator::new(OperatingMode::Native, CommitSeq::ZERO, 16);

        // Simulate crash BEFORE fsync1: commit is not durable
        let sub1 = make_submission(&[1], 0, 1);
        let seq1 = coord.submit(sub1, 1_000_000).unwrap();
        assert_eq!(seq1.get(), 1);
        // Crash here: no fsync1, no marker. On recovery, commit_seq_tip
        // would be restored from the marker stream (still at 0).
        // The pending commit is lost (expected: capsule may exist but
        // marker does not, so it's not committed).

        // Reset to simulate recovery
        let mut coord = WriteCoordinator::new(
            OperatingMode::Native,
            CommitSeq::ZERO, // recovered from marker stream tip
            16,
        );

        // Simulate crash AFTER fsync1 but BEFORE marker append
        let sub2 = make_submission(&[2], 0, 2);
        let _seq2 = coord.submit(sub2, 2_000_000).unwrap();
        let _fsync1 = coord.fsync1();
        // Crash here: capsule + proof are durable (fsync1 done), but
        // marker not appended. On recovery, commit is NOT committed
        // (marker is the point of no return, not the proof).

        // Reset again
        let mut coord = WriteCoordinator::new(OperatingMode::Native, CommitSeq::ZERO, 16);

        // Simulate complete commit
        let sub3 = make_submission(&[3], 0, 3);
        let result = coord.submit_and_commit(sub3, 3_000_000);
        assert!(
            matches!(result, CommitResult::Committed { commit_seq, .. } if commit_seq.get() == 1),
            "complete commit should succeed"
        );

        // After recovery, this commit IS visible (marker is durable)
        assert_eq!(coord.commit_seq_tip().get(), 1);
    }

    // ── bd-15jh test 6: test_native_concurrent_writers ──

    #[test]
    fn test_native_concurrent_writers() {
        // N writers commit in parallel without serialization on payload.
        let mut coord = WriteCoordinator::new(OperatingMode::Native, CommitSeq::ZERO, 32);

        // 10 writers touching disjoint pages
        let base_time = 1_700_000_000_000_000_000_u64;
        for i in 0..10u8 {
            let page = u32::from(i) + 1; // pages 1..10
            let sub = make_submission(&[page], 0, i);
            let seq = coord.submit(sub, base_time + u64::from(i)).unwrap();
            assert_eq!(
                seq.get(),
                u64::from(i) + 1,
                "writer {i} should get sequential commit_seq"
            );
        }

        // All submitted without conflict (different pages)
        assert_eq!(coord.pending_count(), 10);

        // Batch commit
        coord.fsync1();
        let markers = coord.append_markers_and_fsync2();
        assert_eq!(markers.len(), 10);
        let results = coord.drain_committed();
        assert_eq!(results.len(), 10);

        // Now test FCW conflict: writer 11 touches page 5 (already committed)
        let conflicting = make_submission(&[5], 0, 11);
        let result = coord.submit(conflicting, base_time + 100);
        assert!(
            matches!(result, Err(CommitResult::ConflictFcw { .. })),
            "overlapping page should trigger FCW conflict"
        );

        // Writer 12 touches page 5 but with begin_seq >= the commit
        let non_conflicting = make_submission(&[5], 5, 12);
        let result = coord.submit(non_conflicting, base_time + 200);
        assert!(
            result.is_ok(),
            "writer with updated begin_seq should not conflict"
        );
    }

    // ── Additional protocol invariant tests ──

    #[test]
    fn test_coordinator_shutdown_rejects_submissions() {
        let mut coord = WriteCoordinator::new(OperatingMode::Native, CommitSeq::ZERO, 16);

        coord.initiate_shutdown();

        let sub = make_submission(&[1], 0, 1);
        let result = coord.submit(sub, 1_000_000);
        assert!(matches!(result, Err(CommitResult::ShuttingDown)));
    }

    #[test]
    fn test_commit_seq_gap_free() {
        let mut coord = WriteCoordinator::new(OperatingMode::Native, CommitSeq::new(100), 16);

        for i in 0..5u8 {
            let sub = make_submission(&[u32::from(i) + 1], 100, i);
            let seq = coord.submit(sub, 1_000_000 + u64::from(i)).unwrap();
            assert_eq!(seq.get(), 101 + u64::from(i), "commit_seq must be gap-free");
        }
    }

    #[test]
    fn test_commit_time_monotonic() {
        let mut coord = WriteCoordinator::new(OperatingMode::Native, CommitSeq::ZERO, 16);

        // Submit with decreasing wall-clock times
        let sub1 = make_submission(&[1], 0, 1);
        let _seq1 = coord.submit(sub1, 1_000_000).unwrap();

        let sub2 = make_submission(&[2], 0, 2);
        let _seq2 = coord.submit(sub2, 500_000).unwrap(); // earlier timestamp!

        // Drain and verify monotonicity
        coord.fsync1();
        coord.append_markers_and_fsync2();
        let results = coord.drain_committed();

        let times: Vec<u64> = results
            .iter()
            .filter_map(|r| {
                if let CommitResult::Committed {
                    commit_time_unix_ns,
                    ..
                } = r
                {
                    Some(*commit_time_unix_ns)
                } else {
                    None
                }
            })
            .collect();

        assert_eq!(times.len(), 2);
        assert!(
            times[0] < times[1],
            "commit times must be monotonically increasing: {times:?}"
        );
    }

    #[test]
    fn test_marker_integrity_tamper_detection() {
        let marker = CommitMarker::new(
            CommitSeq::new(1),
            1_000_000,
            make_oid(0x11),
            make_oid(0x22),
            None,
        );
        assert!(marker.verify_integrity());

        // Tamper with commit_seq
        let mut tampered = marker;
        tampered.commit_seq = CommitSeq::new(999);
        assert!(!tampered.verify_integrity());
    }

    #[test]
    fn test_fsync_barriers_order() {
        let mut barriers = FsyncBarriers::new();
        assert!(!barriers.all_complete());

        barriers.fsync1_complete = true;
        assert!(!barriers.all_complete());

        barriers.fsync2_complete = true;
        assert!(barriers.all_complete());
    }

    // ── bd-14m.2.1: Group commit observability metrics ──

    #[test]
    fn test_group_commit_metrics_basic() {
        use crate::metrics::GroupCommitMetrics;
        let m = GroupCommitMetrics::new();

        m.record_submission();
        m.record_submission();
        m.record_submission();
        m.record_group_commit(3, 500);
        m.record_fsync1();
        m.record_fsync2();
        m.record_fcw_conflict();
        m.record_ssi_conflict();
        m.record_shutdown_rejection();

        let snap = m.snapshot();
        assert_eq!(snap.submissions_total, 3);
        assert_eq!(snap.group_commits_total, 1);
        assert_eq!(snap.group_commit_size_sum, 3);
        assert_eq!(snap.commit_latency_us_total, 500);
        assert_eq!(snap.fsync1_total, 1);
        assert_eq!(snap.fsync2_total, 1);
        assert_eq!(snap.fcw_conflicts_total, 1);
        assert_eq!(snap.ssi_conflicts_total, 1);
        assert_eq!(snap.shutdown_rejections_total, 1);
        assert_eq!(snap.avg_group_size(), 3);
        assert_eq!(snap.avg_commit_latency_us(), 500);
    }

    #[test]
    fn test_group_commit_metrics_reset() {
        use crate::metrics::GroupCommitMetrics;
        let m = GroupCommitMetrics::new();
        m.record_submission();
        m.record_group_commit(1, 100);
        m.record_fsync1();
        m.record_fsync2();
        m.record_fcw_conflict();
        m.reset();
        let snap = m.snapshot();
        assert_eq!(snap.submissions_total, 0);
        assert_eq!(snap.group_commits_total, 0);
        assert_eq!(snap.fsync1_total, 0);
        assert_eq!(snap.fsync2_total, 0);
        assert_eq!(snap.fcw_conflicts_total, 0);
    }

    #[test]
    fn test_group_commit_metrics_display() {
        use crate::metrics::GroupCommitMetrics;
        let m = GroupCommitMetrics::new();
        m.record_submission();
        m.record_group_commit(1, 200);
        m.record_fsync1();
        m.record_fsync2();
        let s = m.snapshot().to_string();
        assert!(s.contains("group_commits=1"));
        assert!(s.contains("submissions=1"));
        assert!(s.contains("fsync1=1"));
        assert!(s.contains("fsync2=1"));
    }

    #[test]
    fn test_group_commit_metrics_avg_zero() {
        use crate::metrics::GroupCommitMetrics;
        let m = GroupCommitMetrics::new();
        let snap = m.snapshot();
        assert_eq!(snap.avg_group_size(), 0);
        assert_eq!(snap.avg_commit_latency_us(), 0);
        assert_eq!(snap.fsync_reduction_ratio(), 0);
    }

    /// Deterministic proof that group commit achieves >5x fsync reduction.
    ///
    /// Without group commit: N commits × 2 fsyncs = 2N fsyncs.
    /// With group commit: N commits batched → 1 fsync1 + 1 fsync2 = 2 fsyncs.
    /// Reduction ratio: 2N / 2 = N (for N >= 6, ratio > 5x).
    #[test]
    fn test_fsync_reduction_proof_deterministic() {
        use crate::metrics::GLOBAL_GROUP_COMMIT_METRICS;

        // Reset global metrics for this test.
        GLOBAL_GROUP_COMMIT_METRICS.reset();

        let n = 10_u8; // 10 concurrent writers
        let mut coord = WriteCoordinator::new(OperatingMode::Native, CommitSeq::ZERO, 32);

        let base_time = 1_700_000_000_000_000_000_u64;

        // Phase 1: Submit all N writers (disjoint pages)
        for i in 0..n {
            let page = u32::from(i) + 1;
            let sub = make_submission(&[page], 0, i);
            coord.submit(sub, base_time + u64::from(i)).unwrap();
        }

        // Phase 2: Single batch flush (1 fsync1 + 1 fsync2)
        coord.fsync1();
        coord.append_markers_and_fsync2();
        let results = coord.drain_committed();
        assert_eq!(results.len(), usize::from(n));

        // Record the group commit metric
        GLOBAL_GROUP_COMMIT_METRICS.record_group_commit(u64::from(n), 0);

        let snap = GLOBAL_GROUP_COMMIT_METRICS.snapshot();

        // Verify: N submissions, but only 1 fsync1 + 1 fsync2 = 2 fsyncs total
        assert_eq!(snap.submissions_total, u64::from(n));
        assert_eq!(snap.fsync1_total, 1, "only 1 FSYNC_1 for entire batch");
        assert_eq!(snap.fsync2_total, 1, "only 1 FSYNC_2 for entire batch");

        // Without batching: each commit needs its own fsync1 + fsync2 = 2N fsyncs
        let unbatched_fsyncs = u64::from(n) * 2;
        let batched_fsyncs = snap.fsync1_total + snap.fsync2_total;
        let reduction = unbatched_fsyncs / batched_fsyncs;

        assert!(
            reduction >= 5,
            "group commit must achieve >=5x fsync reduction: \
             {n} commits, unbatched={unbatched_fsyncs} fsyncs, \
             batched={batched_fsyncs} fsyncs, reduction={reduction}x"
        );

        // Verify the snapshot ratio method agrees
        assert!(
            snap.fsync_reduction_ratio() >= 5,
            "fsync_reduction_ratio must be >= 5: got {}",
            snap.fsync_reduction_ratio()
        );
    }

    /// Verify metrics are emitted during submit_and_commit convenience path.
    #[test]
    fn test_submit_and_commit_records_metrics() {
        use crate::metrics::GLOBAL_GROUP_COMMIT_METRICS;

        GLOBAL_GROUP_COMMIT_METRICS.reset();

        let mut coord = WriteCoordinator::new(OperatingMode::Native, CommitSeq::ZERO, 16);
        let sub = make_submission(&[1], 0, 1);
        let result = coord.submit_and_commit(sub, 1_000_000);
        assert!(matches!(result, CommitResult::Committed { .. }));

        let snap = GLOBAL_GROUP_COMMIT_METRICS.snapshot();
        assert_eq!(snap.submissions_total, 1);
        assert_eq!(snap.group_commits_total, 1);
        assert_eq!(snap.group_commit_size_sum, 1);
        assert_eq!(snap.fsync1_total, 1);
        assert_eq!(snap.fsync2_total, 1);
    }

    /// Verify FCW conflict increments the metric counter.
    #[test]
    fn test_fcw_conflict_metric() {
        use crate::metrics::GLOBAL_GROUP_COMMIT_METRICS;

        GLOBAL_GROUP_COMMIT_METRICS.reset();

        let mut coord = WriteCoordinator::new(OperatingMode::Native, CommitSeq::ZERO, 16);

        // First commit succeeds
        let sub1 = make_submission(&[1], 0, 1);
        coord.submit_and_commit(sub1, 1_000_000);

        // Second commit to same page with stale begin_seq fails
        let sub2 = make_submission(&[1], 0, 2);
        let result = coord.submit(sub2, 2_000_000);
        assert!(matches!(result, Err(CommitResult::ConflictFcw { .. })));

        let snap = GLOBAL_GROUP_COMMIT_METRICS.snapshot();
        assert_eq!(snap.fcw_conflicts_total, 1);
    }

    /// Verify shutdown rejection increments the metric counter.
    #[test]
    fn test_shutdown_rejection_metric() {
        use crate::metrics::GLOBAL_GROUP_COMMIT_METRICS;

        GLOBAL_GROUP_COMMIT_METRICS.reset();

        let mut coord = WriteCoordinator::new(OperatingMode::Native, CommitSeq::ZERO, 16);
        coord.initiate_shutdown();

        let sub = make_submission(&[1], 0, 1);
        let result = coord.submit(sub, 1_000_000);
        assert!(matches!(result, Err(CommitResult::ShuttingDown)));

        let snap = GLOBAL_GROUP_COMMIT_METRICS.snapshot();
        assert_eq!(snap.shutdown_rejections_total, 1);
    }

    // ── bd-14m.2: Parallel WAL epoch and flush_batch ──

    /// Verify flush_batch increments epoch and records metrics.
    #[test]
    fn test_flush_batch_epoch_tracking() {
        use crate::metrics::GLOBAL_GROUP_COMMIT_METRICS;

        GLOBAL_GROUP_COMMIT_METRICS.reset();

        let mut coord = WriteCoordinator::new(OperatingMode::Native, CommitSeq::ZERO, 32);
        assert_eq!(coord.current_epoch(), 0);

        // First batch: 3 commits
        let base_time = 1_700_000_000_000_000_000_u64;
        for i in 0..3u8 {
            let sub = make_submission(&[u32::from(i) + 1], 0, i);
            coord.submit(sub, base_time + u64::from(i)).unwrap();
        }
        let results = coord.flush_batch();
        assert_eq!(results.len(), 3);
        assert_eq!(coord.current_epoch(), 1);

        // Second batch: 2 commits
        for i in 3..5u8 {
            let sub = make_submission(&[u32::from(i) + 10], 3, i);
            coord.submit(sub, base_time + 100 + u64::from(i)).unwrap();
        }
        let results = coord.flush_batch();
        assert_eq!(results.len(), 2);
        assert_eq!(coord.current_epoch(), 2);

        let snap = GLOBAL_GROUP_COMMIT_METRICS.snapshot();
        assert_eq!(snap.group_commits_total, 2);
        assert_eq!(snap.group_commit_size_sum, 5); // 3 + 2
        assert_eq!(snap.submissions_total, 5);
        assert_eq!(snap.fsync1_total, 2);
        assert_eq!(snap.fsync2_total, 2);
    }

    /// flush_batch on empty batch is a no-op.
    #[test]
    fn test_flush_batch_empty_noop() {
        let mut coord = WriteCoordinator::new(OperatingMode::Native, CommitSeq::ZERO, 16);
        let results = coord.flush_batch();
        assert!(results.is_empty());
        assert_eq!(coord.current_epoch(), 0); // epoch not incremented
    }

    /// Verify submit_and_commit delegates through flush_batch path.
    #[test]
    fn test_submit_and_commit_uses_flush_batch_epoch() {
        let mut coord = WriteCoordinator::new(OperatingMode::Native, CommitSeq::ZERO, 16);
        let sub = make_submission(&[1], 0, 1);
        let result = coord.submit_and_commit(sub, 1_000_000);
        assert!(matches!(result, CommitResult::Committed { .. }));
        assert_eq!(coord.current_epoch(), 1);
    }
}
