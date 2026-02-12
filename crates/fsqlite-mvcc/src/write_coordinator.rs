//! §5.9.1-5.9.2 Write Coordinator: Native Mode Sequencer + Compatibility WAL Path.
//!
//! The write coordinator serializes the commit critical section. In **native mode**
//! (§5.9.1) it never moves page payload bytes — it validates, allocates a
//! `commit_seq`, and appends a tiny `CommitMarker`. In **compatibility mode**
//! (§5.9.2) it additionally serializes WAL append + fsync + version publishing.
//!
//! Multi-process: exactly one lease-backed coordinator process at a time;
//! others route via IPC (§5.9.0, coordinator_ipc module).

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use fsqlite_types::{CommitSeq, IntentOp, ObjectId, PageData, PageNumber, Snapshot, TxnToken};
use parking_lot::RwLock;
use tracing::{debug, info, warn};

use crate::core_types::TransactionMode;
use crate::witness_objects::AbortPolicy;

// ---------------------------------------------------------------------------
// Coordinator Mode
// ---------------------------------------------------------------------------

/// Operating mode for the write coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CoordinatorMode {
    /// §5.9.1: Tiny-marker sequencer. Never moves page payload bytes.
    Native,
    /// §5.9.2: WAL append path. Serializes WAL write + fsync + publish.
    Compatibility,
}

// ---------------------------------------------------------------------------
// §5.9.1 Native Mode Types
// ---------------------------------------------------------------------------

/// Native mode publish request (in-process schema, §5.9.1).
///
/// The coordinator validates using `write_set_summary` and coordinator indexes
/// only — it MUST NOT decode the full capsule during validation.
#[derive(Debug)]
pub struct NativePublishRequest {
    /// Identity of the committing transaction.
    pub txn: TxnToken,
    /// Begin sequence (snapshot lower bound).
    pub begin_seq: CommitSeq,
    /// Object ID of the pre-persisted commit capsule.
    pub capsule_object_id: ObjectId,
    /// BLAKE3-256 digest of capsule bytes (audit/sanity check).
    pub capsule_digest: [u8; 32],
    /// Page numbers in the write set (no false negatives).
    /// Uses `BTreeSet<u32>` as a V1 stand-in for `RoaringBitmap<u32>`.
    pub write_set_summary: BTreeSet<u32>,
    /// Object IDs of read witnesses.
    pub read_witnesses: Vec<ObjectId>,
    /// Object IDs of write witnesses.
    pub write_witnesses: Vec<ObjectId>,
    /// Object IDs of emitted dependency edges.
    pub edge_ids: Vec<ObjectId>,
    /// Object IDs of merge witnesses.
    pub merge_witnesses: Vec<ObjectId>,
    /// Abort policy for this commit.
    pub abort_policy: AbortPolicy,
}

/// Native mode publish response (§5.9.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativePublishResponse {
    /// Commit succeeded: marker appended, commit_seq allocated.
    Ok {
        /// Allocated commit sequence number (monotonically increasing).
        commit_seq: CommitSeq,
        /// Object ID of the persisted commit marker.
        marker_object_id: ObjectId,
    },
    /// Write conflict detected (first-committer-wins).
    Conflict {
        /// Pages that conflict with an already-committed transaction.
        conflicting_pages: Vec<PageNumber>,
        /// The commit_seq of the conflicting transaction.
        conflicting_commit_seq: CommitSeq,
    },
    /// Aborted (e.g., `SQLITE_BUSY_SNAPSHOT` from SSI).
    Aborted {
        /// Error code.
        code: u32,
    },
    /// I/O error during marker append.
    IoError {
        /// Human-readable error description.
        message: String,
    },
}

// ---------------------------------------------------------------------------
// §5.9.2 Compatibility Mode Types
// ---------------------------------------------------------------------------

/// Compatibility mode commit request (in-process schema, §5.9.2).
#[derive(Debug)]
pub struct CompatCommitRequest {
    /// Identity of the committing transaction.
    pub txn: TxnToken,
    /// Transaction mode (Serialized or Concurrent).
    pub mode: TransactionMode,
    /// Pages to be committed (page images).
    pub write_set: CommitWriteSet,
    /// Intent log for audit/merge certificates (§5.10).
    /// Coordinator MUST NOT interpret this for rebase/index-key regen.
    pub intent_log: Vec<IntentOp>,
    /// Page locks held (for release after commit).
    pub page_locks: HashSet<PageNumber>,
    /// Snapshot of the committing transaction.
    pub snapshot: Snapshot,
    /// SSI state: has incoming rw-antidependency edges.
    pub has_in_rw: bool,
    /// SSI state: has outgoing rw-antidependency edges.
    pub has_out_rw: bool,
    /// WAL FEC policy snapshot for this commit group (§3.4.1).
    pub wal_fec_r: u8,
}

/// Compatibility mode commit response (§5.9.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompatCommitResponse {
    /// Commit succeeded: WAL synced, versions published.
    Ok {
        /// WAL offset where the commit record was written.
        wal_offset: u64,
        /// Allocated commit sequence number.
        commit_seq: CommitSeq,
    },
    /// Write conflict detected.
    Conflict {
        /// Pages that conflict.
        conflicting_pages: Vec<PageNumber>,
        /// The commit sequence of the transaction that caused the conflict.
        /// V1 does not track per-page TxnId, so we report CommitSeq instead.
        conflicting_commit_seq: CommitSeq,
    },
    /// I/O error during WAL append/sync.
    IoError {
        /// Human-readable error description.
        message: String,
    },
}

// ---------------------------------------------------------------------------
// CommitWriteSet + Spill Infrastructure
// ---------------------------------------------------------------------------

/// How the coordinator obtains page images for WAL append (§5.9.2).
#[derive(Debug)]
pub enum CommitWriteSet {
    /// Small transactions: page bytes held in memory.
    Inline(HashMap<PageNumber, PageData>),
    /// Large transactions: page bytes spilled to a private file.
    Spilled(SpilledWriteSet),
}

impl CommitWriteSet {
    /// Number of pages in the write set.
    #[must_use]
    pub fn page_count(&self) -> usize {
        match self {
            Self::Inline(pages) => pages.len(),
            Self::Spilled(spilled) => spilled.pages.len(),
        }
    }

    /// Page numbers in the write set.
    #[must_use]
    pub fn page_numbers(&self) -> Vec<PageNumber> {
        match self {
            Self::Inline(pages) => pages.keys().copied().collect(),
            Self::Spilled(spilled) => spilled.pages.keys().copied().collect(),
        }
    }

    /// Whether this write set uses the spill path.
    #[must_use]
    pub const fn is_spilled(&self) -> bool {
        matches!(self, Self::Spilled(_))
    }
}

/// Handle to the spill file backing a `CommitWriteSet::Spilled` (§5.9.2).
#[derive(Debug)]
pub enum SpillHandle {
    /// Coordinator opens by path (single-process or platform fallback).
    Path(PathBuf),
    /// Unix multi-process: coordinator receives an fd via SCM_RIGHTS (§5.9.0).
    #[cfg(target_family = "unix")]
    Fd(std::os::unix::io::OwnedFd),
}

/// Spilled write set: handle + page index (§5.9.2).
#[derive(Debug)]
pub struct SpilledWriteSet {
    /// Readable spill file handle for the duration of the commit.
    pub spill: SpillHandle,
    /// Page index: page number -> location in spill file (last-write-wins).
    pub pages: HashMap<PageNumber, SpillLoc>,
}

/// Location of a page within a spill file (§5.9.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpillLoc {
    /// Byte offset within the spill file.
    pub offset: u64,
    /// Length in bytes (MUST equal page_size in V1).
    pub len: u32,
    /// Integrity hash of the spilled page bytes (`xxh3_64(page_bytes)`).
    pub xxh3_64: u64,
}

// ---------------------------------------------------------------------------
// Coordinator Lease
// ---------------------------------------------------------------------------

/// Default spill threshold: 32 MiB.
pub const DEFAULT_SPILL_THRESHOLD: usize = 32 * 1024 * 1024;

/// Default max batch size for group commit.
pub const DEFAULT_MAX_BATCH_SIZE: usize = 16;

/// Coordinator lease state. Only one coordinator may be active at a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoordinatorLease {
    /// Process ID of the lease holder.
    pub holder_pid: u64,
    /// Monotonic timestamp when the lease was acquired.
    pub acquired_at: u64,
    /// Lease expiry (0 = no expiry, held until explicit release or crash).
    pub expires_at: u64,
}

// ---------------------------------------------------------------------------
// Write Coordinator
// ---------------------------------------------------------------------------

/// The write coordinator serializes commit sequencing.
///
/// Native mode (§5.9.1): validate → allocate `commit_seq` → append tiny marker.
/// Compatibility mode (§5.9.2): validate → WAL append → fsync → publish.
pub struct WriteCoordinator {
    /// Operating mode.
    mode: CoordinatorMode,
    /// Monotonically increasing commit sequence.
    next_commit_seq: AtomicU64,
    /// First-committer-wins index: page number -> most recent commit_seq that
    /// modified that page. Used for O(W) validation.
    commit_page_index: RwLock<HashMap<u32, CommitSeq>>,
    /// Current WAL offset (compatibility mode).
    wal_offset: AtomicU64,
    /// Coordinator lease.
    lease: RwLock<Option<CoordinatorLease>>,
    /// Commit records for publishing (compatibility mode).
    committed_seqs: RwLock<Vec<CommitSeq>>,
}

impl WriteCoordinator {
    /// Create a new write coordinator in the given mode.
    #[must_use]
    pub fn new(mode: CoordinatorMode) -> Self {
        Self {
            mode,
            next_commit_seq: AtomicU64::new(1),
            commit_page_index: RwLock::new(HashMap::new()),
            wal_offset: AtomicU64::new(0),
            lease: RwLock::new(None),
            committed_seqs: RwLock::new(Vec::new()),
        }
    }

    /// Current mode.
    #[must_use]
    pub fn mode(&self) -> CoordinatorMode {
        self.mode
    }

    /// Restore coordinator state from persistent storage (WAL/Marker stream).
    ///
    /// MUST be called immediately after creation/lease-acquisition to populate
    /// the FCW conflict detection index and commit sequence.
    ///
    /// `next_seq` should be the next available commit sequence number (i.e.,
    /// last_committed_seq + 1).
    ///
    /// `recent_commits` should map page numbers to their last modification
    /// commit sequence, derived from the recent history window (covering
    /// at least the oldest active transaction's snapshot).
    pub fn restore_state(
        &self,
        next_seq: CommitSeq,
        recent_commits: HashMap<u32, CommitSeq>,
        wal_offset: u64,
    ) {
        self.next_commit_seq.store(next_seq.get(), Ordering::SeqCst);
        self.wal_offset.store(wal_offset, Ordering::SeqCst);
        let mut index = self.commit_page_index.write();
        *index = recent_commits;
        info!(
            bead_id = "bd-389e",
            next_seq = next_seq.get(),
            wal_offset,
            restored_pages = index.len(),
            "coordinator state restored from persistence"
        );
    }

    /// Acquire the coordinator lease for the given PID.
    ///
    /// Returns `true` if the lease was acquired, `false` if another
    /// process already holds it.
    pub fn acquire_lease(&self, pid: u64, timestamp: u64) -> bool {
        let mut lease = self.lease.write();
        if let Some(existing) = &*lease {
            if existing.expires_at > 0 && existing.expires_at <= timestamp {
                // Lease expired: allow takeover.
                info!(
                    bead_id = "bd-389e",
                    old_pid = existing.holder_pid,
                    new_pid = pid,
                    "coordinator lease expired, allowing takeover"
                );
            } else {
                debug!(
                    bead_id = "bd-389e",
                    holder = existing.holder_pid,
                    "coordinator lease already held"
                );
                return false;
            }
        }
        *lease = Some(CoordinatorLease {
            holder_pid: pid,
            acquired_at: timestamp,
            expires_at: 0, // No expiry by default.
        });
        drop(lease);
        info!(bead_id = "bd-389e", pid, "coordinator lease acquired");
        true
    }

    /// Release the coordinator lease.
    pub fn release_lease(&self, pid: u64) -> bool {
        let mut lease = self.lease.write();
        if let Some(existing) = &*lease {
            if existing.holder_pid == pid {
                *lease = None;
                drop(lease);
                info!(bead_id = "bd-389e", pid, "coordinator lease released");
                return true;
            }
        }
        false
    }

    /// Force-release the coordinator lease (crash recovery / takeover).
    pub fn force_release_lease(&self) {
        let mut lease = self.lease.write();
        if let Some(existing) = &*lease {
            warn!(
                bead_id = "bd-389e",
                pid = existing.holder_pid,
                "coordinator lease force-released (crash recovery)"
            );
        }
        *lease = None;
    }

    // -- §5.9.1 Native Mode State Machine --

    /// Native mode publish: Validate → Seq+Proof → Marker IO → Ok.
    ///
    /// The coordinator MUST NOT decode the full capsule. Validation operates
    /// only on `write_set_summary` and the commit page index.
    pub fn native_publish(&self, req: &NativePublishRequest) -> NativePublishResponse {
        assert_eq!(
            self.mode,
            CoordinatorMode::Native,
            "native_publish called in compatibility mode"
        );

        debug!(
            bead_id = "bd-389e",
            txn = ?req.txn,
            pages = req.write_set_summary.len(),
            "native_publish: starting validation"
        );

        // Step 1: Validate (first-committer-wins on write_set_summary).
        if let Some((conflict_pages, conflict_seq)) =
            self.validate_fcw_set(&req.write_set_summary, req.begin_seq)
        {
            info!(
                bead_id = "bd-389e",
                txn = ?req.txn,
                conflicts = conflict_pages.len(),
                "native_publish: FCW conflict detected"
            );
            return NativePublishResponse::Conflict {
                conflicting_pages: conflict_pages,
                conflicting_commit_seq: conflict_seq,
            };
        }

        // Step 2: Allocate commit_seq.
        let commit_seq = self.allocate_commit_seq();

        // Step 3: Update commit page index.
        self.update_commit_index(&req.write_set_summary, commit_seq);

        // Step 4: "Marker IO" — in the full implementation, this appends a
        // CommitMarker to the marker stream. Here we generate the marker
        // object ID deterministically.
        let marker_object_id = Self::derive_marker_id(req.txn, commit_seq);

        info!(
            bead_id = "bd-389e",
            txn = ?req.txn,
            commit_seq = commit_seq.get(),
            "native_publish: commit approved (marker only, no page bytes)"
        );

        NativePublishResponse::Ok {
            commit_seq,
            marker_object_id,
        }
    }

    // -- §5.9.2 Compatibility Mode State Machine --

    /// Compatibility mode commit: Validate → WALAppend → sync → Publish → Ok.
    pub fn compat_commit(&self, req: &CompatCommitRequest) -> CompatCommitResponse {
        assert_eq!(
            self.mode,
            CoordinatorMode::Compatibility,
            "compat_commit called in native mode"
        );

        let page_numbers: Vec<u32> = req
            .write_set
            .page_numbers()
            .iter()
            .map(|p| p.get())
            .collect();
        let page_set: BTreeSet<u32> = page_numbers.iter().copied().collect();

        debug!(
            bead_id = "bd-389e",
            txn = ?req.txn,
            mode = ?req.mode,
            pages = page_numbers.len(),
            spilled = req.write_set.is_spilled(),
            "compat_commit: starting validation"
        );

        // Step 1: Validate (FCW).
        if let Some((conflict_pages, conflict_seq)) =
            self.validate_fcw_set(&page_set, req.snapshot.high)
        {
            info!(
                bead_id = "bd-389e",
                txn = ?req.txn,
                conflicts = conflict_pages.len(),
                "compat_commit: FCW conflict detected"
            );
            return CompatCommitResponse::Conflict {
                conflicting_pages: conflict_pages,
                conflicting_commit_seq: conflict_seq,
            };
        }

        // Step 2: Allocate commit_seq.
        let commit_seq = self.allocate_commit_seq();

        // Step 3: WAL Append — compute offset and record it.
        // In the full implementation, this writes page frames to the WAL file.
        // Frame size per page: 24-byte header + page_size bytes.
        let frame_header_size = 24_u64;
        let page_size = Self::infer_page_size(&req.write_set);
        let batch_bytes = page_numbers.len() as u64 * (frame_header_size + page_size);
        let wal_offset = self.wal_offset.fetch_add(batch_bytes, Ordering::SeqCst);

        // Step 4: "sync" — fsync placeholder. In the full implementation,
        // this is the group commit fsync point.

        // Step 5: Update commit index (publish).
        self.update_commit_index(&page_set, commit_seq);

        // Track committed seq.
        self.committed_seqs.write().push(commit_seq);

        info!(
            bead_id = "bd-389e",
            txn = ?req.txn,
            commit_seq = commit_seq.get(),
            wal_offset,
            pages = page_numbers.len(),
            "compat_commit: commit approved (WAL path)"
        );

        CompatCommitResponse::Ok {
            wal_offset,
            commit_seq,
        }
    }

    // -- Batch commit (group commit optimization) --

    /// Process a batch of compatibility mode requests (group commit, §5.9.2).
    ///
    /// Phases: validate all → WAL append all → single fsync → publish all.
    /// Returns one response per request.
    pub fn compat_commit_batch(
        &self,
        requests: &[CompatCommitRequest],
    ) -> Vec<CompatCommitResponse> {
        assert_eq!(
            self.mode,
            CoordinatorMode::Compatibility,
            "compat_commit_batch called in native mode"
        );

        let mut responses = Vec::with_capacity(requests.len());
        let mut accepted_commits: Vec<(CommitSeq, BTreeSet<u32>)> = Vec::new();
        let mut batch_page_owner: HashMap<u32, CommitSeq> = HashMap::new();
        let frame_header_size = 24_u64;
        let mut total_batch_bytes = 0_u64;

        // Phase 1: Validate all.
        for req in requests {
            let page_numbers: Vec<u32> = req
                .write_set
                .page_numbers()
                .iter()
                .map(|p| p.get())
                .collect();
            let page_set: BTreeSet<u32> = page_numbers.iter().copied().collect();

            if let Some((conflict_pages, conflict_seq)) =
                self.validate_fcw_set(&page_set, req.snapshot.high)
            {
                responses.push(CompatCommitResponse::Conflict {
                    conflicting_pages: conflict_pages,
                    conflicting_commit_seq: conflict_seq,
                });
            } else {
                let mut intra_batch_conflicts = Vec::new();
                let mut intra_batch_conflict_seq = CommitSeq::new(0);
                for &pgno in &page_set {
                    if let Some(&owner_seq) = batch_page_owner.get(&pgno) {
                        if let Some(page) = PageNumber::new(pgno) {
                            intra_batch_conflicts.push(page);
                        }
                        if owner_seq.get() > intra_batch_conflict_seq.get() {
                            intra_batch_conflict_seq = owner_seq;
                        }
                    }
                }

                if intra_batch_conflicts.is_empty() {
                    // Phase 2: Allocate commit_seq and WAL offset.
                    let commit_seq = self.allocate_commit_seq();
                    let page_size = Self::infer_page_size(&req.write_set);
                    let page_count = req.write_set.page_count() as u64;
                    let commit_bytes = page_count * (frame_header_size + page_size);
                    let wal_offset = self.wal_offset.fetch_add(commit_bytes, Ordering::SeqCst);
                    total_batch_bytes += commit_bytes;

                    for &pgno in &page_set {
                        batch_page_owner.insert(pgno, commit_seq);
                    }
                    accepted_commits.push((commit_seq, page_set));
                    responses.push(CompatCommitResponse::Ok {
                        wal_offset,
                        commit_seq,
                    });
                } else {
                    responses.push(CompatCommitResponse::Conflict {
                        conflicting_pages: intra_batch_conflicts,
                        conflicting_commit_seq: intra_batch_conflict_seq,
                    });
                }
            }
        }

        if accepted_commits.is_empty() {
            return responses;
        }

        let accepted_count = accepted_commits.len();

        // Phase 3: Single fsync (placeholder).
        debug!(
            bead_id = "bd-389e",
            batch_size = accepted_count,
            total_bytes = total_batch_bytes,
            "compat_commit_batch: single fsync for batch"
        );

        // Phase 4: Publish all and fill responses.
        for (commit_seq, page_set) in &accepted_commits {
            self.update_commit_index(page_set, *commit_seq);
            self.committed_seqs.write().push(*commit_seq);
        }

        info!(
            bead_id = "bd-389e",
            batch_size = accepted_count,
            conflicts = requests.len() - accepted_count,
            "compat_commit_batch: group commit complete"
        );

        responses
    }

    // -- Internal helpers --

    /// Validate first-committer-wins against the commit page index.
    ///
    /// Returns `None` if no conflicts; `Some((pages, seq))` on conflict.
    fn validate_fcw_set(
        &self,
        write_pages: &BTreeSet<u32>,
        begin_seq: CommitSeq,
    ) -> Option<(Vec<PageNumber>, CommitSeq)> {
        let index = self.commit_page_index.read();
        let mut conflict_pages = Vec::new();
        let mut conflict_seq = CommitSeq::new(0);

        for &pgno in write_pages {
            if let Some(&committed_seq) = index.get(&pgno) {
                if committed_seq.get() > begin_seq.get() {
                    if let Some(pn) = PageNumber::new(pgno) {
                        conflict_pages.push(pn);
                    }
                    if committed_seq.get() > conflict_seq.get() {
                        conflict_seq = committed_seq;
                    }
                }
            }
        }

        if conflict_pages.is_empty() {
            None
        } else {
            Some((conflict_pages, conflict_seq))
        }
    }

    /// Allocate the next commit sequence number (monotonic).
    fn allocate_commit_seq(&self) -> CommitSeq {
        let seq = self.next_commit_seq.fetch_add(1, Ordering::SeqCst);
        CommitSeq::new(seq)
    }

    /// Update the commit page index after a successful commit.
    fn update_commit_index(&self, pages: &BTreeSet<u32>, commit_seq: CommitSeq) {
        let mut index = self.commit_page_index.write();
        for &pgno in pages {
            index.insert(pgno, commit_seq);
        }
    }

    /// Infer page size from the write set (V1: assume 4096 if no data).
    fn infer_page_size(write_set: &CommitWriteSet) -> u64 {
        match write_set {
            CommitWriteSet::Inline(pages) => {
                pages.values().next().map_or(4096, |pd| pd.len() as u64)
            }
            CommitWriteSet::Spilled(spilled) => spilled
                .pages
                .values()
                .next()
                .map_or(4096, |loc| u64::from(loc.len)),
        }
    }

    /// Derive a deterministic marker object ID from txn + commit_seq.
    fn derive_marker_id(txn: TxnToken, commit_seq: CommitSeq) -> ObjectId {
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&txn.id.get().to_le_bytes());
        bytes[8..16].copy_from_slice(&commit_seq.get().to_le_bytes());
        ObjectId::from_bytes(bytes)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::too_many_lines)]
mod tests {
    use super::*;
    use fsqlite_types::{SchemaEpoch, TxnEpoch, TxnId};

    fn test_token(id: u64) -> TxnToken {
        TxnToken::new(TxnId::new(id).unwrap(), TxnEpoch::new(0))
    }

    fn test_snapshot(high: u64) -> Snapshot {
        Snapshot {
            high: CommitSeq::new(high),
            schema_epoch: SchemaEpoch::new(1),
        }
    }

    fn test_page_data(pgno: u32) -> PageData {
        let mut data = vec![0u8; 4096];
        data[..4].copy_from_slice(&pgno.to_le_bytes());
        PageData::from_vec(data)
    }

    fn inline_write_set(pages: &[u32]) -> CommitWriteSet {
        let mut map = HashMap::new();
        for &pgno in pages {
            map.insert(PageNumber::new(pgno).unwrap(), test_page_data(pgno));
        }
        CommitWriteSet::Inline(map)
    }

    // -- §5.9.1 test 1: Native sequencer writes only marker, not page data --

    #[test]
    fn test_native_sequencer_tiny_marker() {
        let coord = WriteCoordinator::new(CoordinatorMode::Native);
        coord.acquire_lease(1, 0);

        let req = NativePublishRequest {
            txn: test_token(1),
            begin_seq: CommitSeq::new(0),
            capsule_object_id: ObjectId::from_bytes([1u8; 16]),
            capsule_digest: [0xAB; 32],
            write_set_summary: BTreeSet::from([5, 10, 15]),
            read_witnesses: vec![ObjectId::from_bytes([2u8; 16])],
            write_witnesses: vec![ObjectId::from_bytes([3u8; 16])],
            edge_ids: Vec::new(),
            merge_witnesses: Vec::new(),
            abort_policy: AbortPolicy::AbortPivot,
        };

        let resp = coord.native_publish(&req);

        // Key assertion: the coordinator returns Ok with a commit_seq and
        // marker_object_id. It NEVER touched page payload bytes — only the
        // write_set_summary (a set of page numbers) was inspected.
        match resp {
            NativePublishResponse::Ok {
                commit_seq,
                marker_object_id,
            } => {
                assert!(commit_seq.get() > 0, "commit_seq must be positive");
                assert_ne!(
                    marker_object_id,
                    ObjectId::from_bytes([0u8; 16]),
                    "marker must be non-zero"
                );
            }
            other => panic!("expected Ok, got {other:?}"),
        }

        // The request has no PageData field — the coordinator physically
        // cannot access page bytes. This is the "tiny marker" guarantee.
    }

    // -- §5.9.2 test 2: Group commit batches fsync --

    #[test]
    fn test_compat_group_commit() {
        let coord = WriteCoordinator::new(CoordinatorMode::Compatibility);
        coord.acquire_lease(1, 0);

        // Create 3 concurrent commit requests to different pages.
        let requests: Vec<CompatCommitRequest> = (1..=3_u64)
            .map(|i| {
                #[allow(clippy::cast_possible_truncation)]
                let pgno = (i as u32) * 10;
                CompatCommitRequest {
                    txn: test_token(i),
                    mode: TransactionMode::Concurrent,
                    write_set: inline_write_set(&[pgno]),
                    intent_log: Vec::new(),
                    page_locks: HashSet::from([PageNumber::new(pgno).unwrap()]),
                    snapshot: test_snapshot(0),
                    has_in_rw: false,
                    has_out_rw: false,
                    wal_fec_r: 0,
                }
            })
            .collect();

        let responses = coord.compat_commit_batch(&requests);

        // All 3 should succeed (different pages, no conflicts).
        assert_eq!(responses.len(), 3);
        let mut commit_seqs = Vec::new();
        for resp in &responses {
            match resp {
                CompatCommitResponse::Ok { commit_seq, .. } => {
                    commit_seqs.push(commit_seq.get());
                }
                other => panic!("expected Ok, got {other:?}"),
            }
        }

        // Commit sequences must be monotonically increasing.
        for window in commit_seqs.windows(2) {
            assert!(window[0] < window[1], "commit_seqs must be monotonic");
        }

        // Key assertion: group commit processes all 3 in a single batch.
        // In a full implementation, this means a single fsync for all 3.
    }

    // -- §5.9.2 test 3: Write-set spill --

    #[test]
    fn test_write_set_spill() {
        // Verify that spilled write sets are handled correctly.
        let spill_loc = SpillLoc {
            offset: 0,
            len: 4096,
            xxh3_64: 0xDEAD_BEEF,
        };

        let spill = SpilledWriteSet {
            spill: SpillHandle::Path(PathBuf::from("/tmp/test-spill.dat")),
            pages: HashMap::from([(PageNumber::new(5).unwrap(), spill_loc)]),
        };

        let write_set = CommitWriteSet::Spilled(spill);
        assert!(write_set.is_spilled());
        assert_eq!(write_set.page_count(), 1);
        assert_eq!(write_set.page_numbers().len(), 1);

        // Spilled write set with the coordinator.
        let coord = WriteCoordinator::new(CoordinatorMode::Compatibility);
        coord.acquire_lease(1, 0);

        let req = CompatCommitRequest {
            txn: test_token(1),
            mode: TransactionMode::Concurrent,
            write_set,
            intent_log: Vec::new(),
            page_locks: HashSet::from([PageNumber::new(5).unwrap()]),
            snapshot: test_snapshot(0),
            has_in_rw: false,
            has_out_rw: false,
            wal_fec_r: 0,
        };

        let resp = coord.compat_commit(&req);
        match resp {
            CompatCommitResponse::Ok { commit_seq, .. } => {
                assert!(commit_seq.get() > 0);
            }
            other => panic!("expected Ok, got {other:?}"),
        }
    }

    // -- test 4: Coordinator lease (single coordinator) --

    #[test]
    fn test_coordinator_lease() {
        let coord = WriteCoordinator::new(CoordinatorMode::Native);

        // First process acquires lease.
        assert!(coord.acquire_lease(100, 0), "first acquire should succeed");

        // Second process cannot acquire while first holds it.
        assert!(!coord.acquire_lease(200, 1), "second acquire should fail");

        // First process releases.
        assert!(coord.release_lease(100), "release by holder should succeed");

        // Now second can acquire.
        assert!(
            coord.acquire_lease(200, 2),
            "acquire after release should succeed"
        );

        // Wrong PID cannot release.
        assert!(
            !coord.release_lease(999),
            "release by non-holder should fail"
        );
    }

    // -- test 5: Coordinator role takeover (crash recovery) --

    #[test]
    fn test_coordinator_role_takeover() {
        let coord = WriteCoordinator::new(CoordinatorMode::Native);

        // Process 100 acquires lease.
        assert!(coord.acquire_lease(100, 0));

        // Process 100 "crashes" — force release.
        coord.force_release_lease();

        // Process 200 can now acquire.
        assert!(
            coord.acquire_lease(200, 1),
            "takeover after force-release should succeed"
        );
    }

    // -- test 6: WAL frame format (page count and offset tracking) --

    #[test]
    fn test_wal_frame_format() {
        let coord = WriteCoordinator::new(CoordinatorMode::Compatibility);
        coord.acquire_lease(1, 0);

        // Commit with 3 pages of 4096 bytes each.
        let req = CompatCommitRequest {
            txn: test_token(1),
            mode: TransactionMode::Serialized,
            write_set: inline_write_set(&[1, 2, 3]),
            intent_log: Vec::new(),
            page_locks: HashSet::from([
                PageNumber::new(1).unwrap(),
                PageNumber::new(2).unwrap(),
                PageNumber::new(3).unwrap(),
            ]),
            snapshot: test_snapshot(0),
            has_in_rw: false,
            has_out_rw: false,
            wal_fec_r: 0,
        };

        let resp = coord.compat_commit(&req);
        match resp {
            CompatCommitResponse::Ok {
                wal_offset,
                commit_seq,
            } => {
                // WAL offset for first commit should be 0 (start of WAL).
                assert_eq!(wal_offset, 0, "first commit starts at WAL offset 0");
                assert!(commit_seq.get() > 0);

                // Each frame = 24-byte header + 4096-byte page = 4120 bytes.
                // 3 frames = 12360 bytes. Next commit should start at 12360.
                let expected_next = 3 * (24 + 4096);
                assert_eq!(
                    coord.wal_offset.load(Ordering::SeqCst),
                    expected_next,
                    "WAL offset advances by frame_header + page_size per page"
                );
            }
            other => panic!("expected Ok, got {other:?}"),
        }

        // Second commit should start where the first ended.
        let req2 = CompatCommitRequest {
            txn: test_token(2),
            mode: TransactionMode::Serialized,
            write_set: inline_write_set(&[4]),
            intent_log: Vec::new(),
            page_locks: HashSet::from([PageNumber::new(4).unwrap()]),
            snapshot: test_snapshot(0),
            has_in_rw: false,
            has_out_rw: false,
            wal_fec_r: 0,
        };

        let resp2 = coord.compat_commit(&req2);
        match resp2 {
            CompatCommitResponse::Ok { wal_offset, .. } => {
                assert_eq!(
                    wal_offset,
                    3 * (24 + 4096),
                    "second commit starts after first"
                );
            }
            other => panic!("expected Ok, got {other:?}"),
        }
    }

    #[test]
    fn test_compat_group_commit_intra_batch_conflict_first_wins() {
        let coord = WriteCoordinator::new(CoordinatorMode::Compatibility);
        coord.acquire_lease(1, 0);

        // Two requests in the same batch write the same page.
        let requests = vec![
            CompatCommitRequest {
                txn: test_token(1),
                mode: TransactionMode::Concurrent,
                write_set: inline_write_set(&[42]),
                intent_log: Vec::new(),
                page_locks: HashSet::from([PageNumber::new(42).unwrap()]),
                snapshot: test_snapshot(0),
                has_in_rw: false,
                has_out_rw: false,
                wal_fec_r: 0,
            },
            CompatCommitRequest {
                txn: test_token(2),
                mode: TransactionMode::Concurrent,
                write_set: inline_write_set(&[42]),
                intent_log: Vec::new(),
                page_locks: HashSet::from([PageNumber::new(42).unwrap()]),
                snapshot: test_snapshot(0),
                has_in_rw: false,
                has_out_rw: false,
                wal_fec_r: 0,
            },
        ];

        let responses = coord.compat_commit_batch(&requests);
        assert_eq!(responses.len(), 2);

        let first_commit_seq = match &responses[0] {
            CompatCommitResponse::Ok { commit_seq, .. } => *commit_seq,
            other => panic!("expected first response Ok, got {other:?}"),
        };

        match &responses[1] {
            CompatCommitResponse::Conflict {
                conflicting_pages,
                conflicting_commit_seq,
            } => {
                assert_eq!(conflicting_pages, &vec![PageNumber::new(42).unwrap()]);
                assert_eq!(*conflicting_commit_seq, first_commit_seq);
            }
            other => panic!("expected second response Conflict, got {other:?}"),
        }
    }

    #[test]
    fn test_restore_state_restores_wal_offset() {
        let coord = WriteCoordinator::new(CoordinatorMode::Compatibility);
        coord.acquire_lease(1, 0);
        coord.restore_state(CommitSeq::new(11), HashMap::new(), 12_345);

        let req = CompatCommitRequest {
            txn: test_token(77),
            mode: TransactionMode::Serialized,
            write_set: inline_write_set(&[7]),
            intent_log: Vec::new(),
            page_locks: HashSet::from([PageNumber::new(7).unwrap()]),
            snapshot: test_snapshot(0),
            has_in_rw: false,
            has_out_rw: false,
            wal_fec_r: 0,
        };

        match coord.compat_commit(&req) {
            CompatCommitResponse::Ok {
                wal_offset,
                commit_seq,
            } => {
                assert_eq!(wal_offset, 12_345);
                assert_eq!(commit_seq, CommitSeq::new(11));
            }
            other => panic!("expected Ok, got {other:?}"),
        }
    }
}
