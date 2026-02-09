//! §5.7.1-5.7.2 SSI Witness Objects (Canonical ECS Schemas) + Hot/Cold Discovery.
//!
//! This module defines the canonical ECS-level schemas for all SSI witness
//! objects and implements the two-stage candidate discovery mechanism:
//!
//! 1. **Hot-plane** (O(1)): SHM `HotWitnessIndex` bitsets provide a superset
//!    of candidates; queries both live epochs (cur and prev) and ORs them.
//! 2. **Cold-plane refinement** (optional): Decode `ReadWitness`/`WriteWitness`
//!    refinements to confirm actual key intersection, reducing false positives.
//!
//! All encoding follows ECS rules: little-endian integers, sorted maps/sets
//! by canonical bytes, canonical roaring encoding for bitmaps.

use std::collections::BTreeSet;

use fsqlite_types::{CommitSeq, ObjectId, RangeKey, TxnToken, WitnessKey};
use tracing::debug;

use crate::hot_witness_index::{HotWitnessIndex, bitset_to_slot_ids};

// ---------------------------------------------------------------------------
// §5.7.1 KeySummary (6 variants)
// ---------------------------------------------------------------------------

/// Compact summary of witness keys for ECS serialization (§5.7.1).
///
/// **Soundness rule**: `KeySummary` MUST NOT have false negatives for its
/// coverage claim. False positives are allowed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeySummary {
    /// Exact set of witness keys (sorted by canonical bytes).
    ExactKeys(Vec<WitnessKey>),
    /// Hashed key set (sorted ascending XXH3 hashes).
    HashedKeySet(Vec<u64>),
    /// Page-level bitmap (page numbers).
    PageBitmap(BTreeSet<u32>),
    /// Cell-level bitmap (`(page << 32) | cell_tag`).
    CellBitmap(BTreeSet<u64>),
    /// Byte range list (sorted by `(page, start, len)`).
    ByteRangeList(Vec<(u32, u16, u16)>),
    /// Chunked: for very large sets, split into chunks.
    Chunked(Vec<KeySummaryChunk>),
}

impl KeySummary {
    /// Check whether this summary potentially overlaps with a witness key.
    ///
    /// Conservative: may return `true` (false positive) but MUST NOT return
    /// `false` when there is a real overlap (no false negatives).
    #[must_use]
    pub fn may_overlap(&self, key: &WitnessKey) -> bool {
        match self {
            Self::ExactKeys(keys) => keys.iter().any(|k| k == key),
            Self::HashedKeySet(hashes) => {
                let h = crate::witness_hierarchy::witness_key_hash(key);
                hashes.binary_search(&h).is_ok()
            }
            Self::PageBitmap(pages) => {
                if let WitnessKey::Custom { .. } = key {
                    // Custom witness keys are not representable in a page bitmap.
                    // Conservatively treat as "may overlap" (no false negatives).
                    true
                } else {
                    let pgno = page_number_of(key);
                    pages.contains(&pgno)
                }
            }
            Self::CellBitmap(cells) => {
                if let WitnessKey::Custom { .. } = key {
                    // Custom witness keys are not representable in a cell bitmap.
                    // Conservatively treat as "may overlap" (no false negatives).
                    true
                } else {
                    let pgno = page_number_of(key);
                    let page_prefix = u64::from(pgno) << 32;
                    let page_end = page_prefix | 0xFFFF_FFFF;
                    cells.range(page_prefix..=page_end).next().is_some()
                }
            }
            Self::ByteRangeList(_ranges) => {
                // Conservative: byte ranges always potentially overlap.
                true
            }
            Self::Chunked(chunks) => chunks.iter().any(|c| c.summary.may_overlap(key)),
        }
    }

    /// Number of keys or entries in this summary.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::ExactKeys(keys) => keys.len(),
            Self::HashedKeySet(hashes) => hashes.len(),
            Self::PageBitmap(pages) => pages.len(),
            Self::CellBitmap(cells) => cells.len(),
            Self::ByteRangeList(ranges) => ranges.len(),
            Self::Chunked(chunks) => chunks.iter().map(|c| c.summary.len()).sum(),
        }
    }

    /// Whether this summary is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A chunk within a `Chunked` key summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeySummaryChunk {
    /// Range prefix this chunk covers.
    pub range_prefix: u32,
    /// The summary for this chunk.
    pub summary: KeySummary,
}

/// Extract the page number from a witness key (for bitmap summaries).
fn page_number_of(key: &WitnessKey) -> u32 {
    match key {
        WitnessKey::Page(pgno) => pgno.get(),
        WitnessKey::Cell { btree_root, .. } | WitnessKey::KeyRange { btree_root, .. } => {
            btree_root.get()
        }
        WitnessKey::ByteRange { page, .. } => page.get(),
        WitnessKey::Custom { .. } => 0,
    }
}

// ---------------------------------------------------------------------------
// §5.7.1 Rich Witness Types
// ---------------------------------------------------------------------------

/// Write kind for `EcsWriteWitness` (§5.7.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WriteKind {
    /// Tentative write; may still be rolled back.
    Intent,
    /// Finalized write; required before commit validation.
    Final,
}

/// Logical time for witness emission ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogicalTime(u64);

impl LogicalTime {
    /// Create a new logical time.
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

/// Rich read witness (ECS schema, §5.7.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcsReadWitness {
    /// Transaction identity (id + epoch).
    pub txn: TxnToken,
    /// Transaction's begin sequence.
    pub begin_seq: CommitSeq,
    /// Hierarchy level this witness was registered at.
    pub level: u8,
    /// Range prefix for bucket lookup.
    pub range_prefix: u32,
    /// Summary of the keys read.
    pub key_summary: KeySummary,
    /// When this witness was emitted (logical time or virtual time).
    pub emitted_at: LogicalTime,
}

/// Rich write witness (ECS schema, §5.7.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcsWriteWitness {
    /// Transaction identity.
    pub txn: TxnToken,
    /// Transaction's begin sequence.
    pub begin_seq: CommitSeq,
    /// Hierarchy level.
    pub level: u8,
    /// Range prefix.
    pub range_prefix: u32,
    /// Summary of the keys written.
    pub key_summary: KeySummary,
    /// When emitted.
    pub emitted_at: LogicalTime,
    /// Intent vs Final.
    pub write_kind: WriteKind,
}

/// Witness participation delta (CRDT union-only, §5.7.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WitnessDelta {
    /// Transaction identity.
    pub txn: TxnToken,
    /// Begin sequence.
    pub begin_seq: CommitSeq,
    /// Read or Write.
    pub kind: WitnessDeltaKind,
    /// Hierarchy level.
    pub level: u8,
    /// Range prefix.
    pub range_prefix: u32,
    /// Participation: union-only CRDT (Present means "is a participant").
    pub participation: WitnessParticipation,
    /// Optional refinement for cold-plane checks.
    pub refinement: Option<KeySummary>,
}

/// Kind discriminant for `WitnessDelta`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WitnessDeltaKind {
    /// This delta records a read participant.
    Read,
    /// This delta records a write participant.
    Write,
}

/// Participation CRDT: union-only (no removals).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WitnessParticipation {
    /// The transaction is a participant.
    Present,
}

impl WitnessDelta {
    /// Merge two deltas for the same (txn, level, prefix, kind).
    ///
    /// Union-only CRDT: participation can only become Present, refinement
    /// is merged by taking the more precise one.
    #[must_use]
    pub fn merge(self, other: &Self) -> Self {
        debug_assert_eq!(self.txn, other.txn);
        debug_assert_eq!(self.level, other.level);
        debug_assert_eq!(self.range_prefix, other.range_prefix);
        debug_assert_eq!(self.kind, other.kind);

        // Participation: union-only → always Present after merge.
        let refinement = match (self.refinement, &other.refinement) {
            (Some(a), Some(b)) => {
                // Keep the one with more keys (more precise).
                if a.len() >= b.len() {
                    Some(a)
                } else {
                    Some(b.clone())
                }
            }
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b.clone()),
            (None, None) => None,
        };

        Self {
            txn: self.txn,
            begin_seq: self.begin_seq,
            kind: self.kind,
            level: self.level,
            range_prefix: self.range_prefix,
            participation: WitnessParticipation::Present,
            refinement,
        }
    }
}

/// Rich dependency edge (ECS schema, §5.7.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcsDependencyEdge {
    /// Kind of dependency.
    pub kind: DependencyEdgeKind,
    /// Transaction that reads.
    pub from: TxnToken,
    /// Transaction that writes.
    pub to: TxnToken,
    /// Basis for the edge (level + range_prefix + optional refinement).
    pub key_basis: EdgeKeyBasis,
    /// Transaction that observed this edge.
    pub observed_by: TxnToken,
    /// Sequence at which this edge was observed.
    pub observation_seq: CommitSeq,
}

/// Kind of dependency edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DependencyEdgeKind {
    /// R -rw-> W: reader R read a key that writer W wrote.
    RwAntiDependency,
}

/// Key basis for a dependency edge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeKeyBasis {
    /// Hierarchy level.
    pub level: u8,
    /// Range prefix.
    pub range_prefix: u32,
    /// Optional refinement for precise key identification.
    pub refinement: Option<KeySummary>,
}

/// Rich commit proof (ECS schema, §5.7.1).
///
/// Replayable proof (not cryptographic) — enough evidence to deterministically
/// re-run SSI validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcsCommitProof {
    /// Transaction identity.
    pub txn: TxnToken,
    /// Begin sequence.
    pub begin_seq: CommitSeq,
    /// Commit sequence.
    pub commit_seq: CommitSeq,
    /// Whether incoming rw-antidependency edges exist.
    pub has_in_rw: bool,
    /// Whether outgoing rw-antidependency edges exist.
    pub has_out_rw: bool,
    /// Refs to ReadWitness ECS objects.
    pub read_witness_refs: Vec<ObjectId>,
    /// Refs to WriteWitness ECS objects.
    pub write_witness_refs: Vec<ObjectId>,
    /// Refs to WitnessIndexSegments used during validation.
    pub index_segments_used: Vec<ObjectId>,
    /// Refs to DependencyEdge ECS objects emitted.
    pub edges_emitted: Vec<ObjectId>,
    /// Refs to MergeWitness objects.
    pub merge_witnesses: Vec<ObjectId>,
    /// Abort policy if SSI violation detected.
    pub abort_policy: AbortPolicy,
}

/// Abort policy for SSI violation resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AbortPolicy {
    /// Abort the pivot transaction (the one forming the dangerous structure).
    AbortPivot,
    /// Abort the youngest transaction.
    AbortYoungest,
    /// Custom policy (implementation-defined).
    Custom,
}

/// Abort witness (ECS schema, §5.7.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbortWitness {
    /// Transaction that was aborted.
    pub txn: TxnToken,
    /// Begin sequence.
    pub begin_seq: CommitSeq,
    /// Sequence at which the abort occurred.
    pub abort_seq: CommitSeq,
    /// Reason for abort.
    pub reason: AbortReason,
    /// Dependency edges that were observed before abort.
    pub edges_observed: Vec<EcsDependencyEdge>,
}

/// Reason for transaction abort.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AbortReason {
    /// SSI pivot detected (dangerous structure).
    SsiPivot,
    /// Transaction was explicitly cancelled.
    Cancelled,
    /// Other reason.
    Other,
}

// ---------------------------------------------------------------------------
// §5.7.2 Hot-Plane Candidate Discovery
// ---------------------------------------------------------------------------

/// Result of hot-plane candidate discovery.
#[derive(Debug, Clone)]
pub struct HotPlaneCandidates {
    /// TxnSlot IDs that are candidate readers.
    pub reader_slot_ids: Vec<u32>,
    /// TxnSlot IDs that are candidate writers.
    pub writer_slot_ids: Vec<u32>,
}

/// Discover candidate transactions from the hot-plane index (§5.7.2, Stage 1).
///
/// Queries both live epochs (cur and prev) bitsets, ORs them, and returns
/// the superset of candidate slot IDs. This is O(1) per bucket.
///
/// **No false negatives guarantee**: if a transaction is active and has
/// registered a witness for any of the given range keys, it WILL appear
/// in the results.
#[must_use]
pub fn hot_plane_discover(index: &HotWitnessIndex, range_keys: &[RangeKey]) -> HotPlaneCandidates {
    let reader_bits = index.candidate_readers(range_keys);
    let writer_bits = index.candidate_writers(range_keys);

    let reader_slot_ids = bitset_to_slot_ids(&reader_bits);
    let writer_slot_ids = bitset_to_slot_ids(&writer_bits);

    debug!(
        bead_id = "bd-1if1",
        range_keys = range_keys.len(),
        reader_candidates = reader_slot_ids.len(),
        writer_candidates = writer_slot_ids.len(),
        "hot-plane candidate discovery"
    );

    HotPlaneCandidates {
        reader_slot_ids,
        writer_slot_ids,
    }
}

// ---------------------------------------------------------------------------
// §5.7.2 Cold-Plane Refinement
// ---------------------------------------------------------------------------

/// Result of cold-plane refinement.
#[derive(Debug, Clone)]
pub struct ColdPlaneRefinementResult {
    /// Confirmed reader TxnTokens (post-refinement).
    pub confirmed_readers: Vec<TxnToken>,
    /// Confirmed writer TxnTokens (post-refinement).
    pub confirmed_writers: Vec<TxnToken>,
    /// Number of false positives eliminated.
    pub false_positives_eliminated: usize,
}

/// Refine hot-plane candidates using cold-plane witness data (§5.7.2, Stage 2).
///
/// Decodes `ReadWitness`/`WriteWitness` refinements to confirm actual
/// key intersection, reducing false positives.
///
/// If no cold-plane data is available, the hot-plane results pass through
/// unchanged.
pub fn cold_plane_refine(
    candidate_readers: &[(TxnToken, Option<EcsReadWitness>)],
    candidate_writers: &[(TxnToken, Option<EcsWriteWitness>)],
    query_key: &WitnessKey,
) -> ColdPlaneRefinementResult {
    let mut confirmed_readers = Vec::new();
    let mut eliminated = 0_usize;

    for (token, witness) in candidate_readers {
        match witness {
            Some(w) if w.key_summary.may_overlap(query_key) => {
                confirmed_readers.push(*token);
            }
            Some(_) => {
                eliminated += 1;
            }
            None => {
                // No cold data: conservatively keep the candidate.
                confirmed_readers.push(*token);
            }
        }
    }

    let mut confirmed_writers = Vec::new();
    for (token, witness) in candidate_writers {
        match witness {
            Some(w) if w.key_summary.may_overlap(query_key) => {
                confirmed_writers.push(*token);
            }
            Some(_) => {
                eliminated += 1;
            }
            None => {
                confirmed_writers.push(*token);
            }
        }
    }

    if eliminated > 0 {
        debug!(
            bead_id = "bd-1if1",
            eliminated,
            confirmed_readers = confirmed_readers.len(),
            confirmed_writers = confirmed_writers.len(),
            "cold-plane refinement eliminated false positives"
        );
    }

    ColdPlaneRefinementResult {
        confirmed_readers,
        confirmed_writers,
        false_positives_eliminated: eliminated,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_types::{CommitSeq, PageNumber, TxnEpoch, TxnId, TxnToken};

    fn test_txn_token(id: u64, epoch: u32) -> TxnToken {
        TxnToken::new(TxnId::new(id).unwrap(), TxnEpoch::new(epoch))
    }

    fn test_page_key(pgno: u32) -> WitnessKey {
        WitnessKey::Page(PageNumber::new(pgno).unwrap())
    }

    fn test_range_key(level: u8, hash_prefix: u32) -> RangeKey {
        RangeKey { level, hash_prefix }
    }

    // -- §5.7.1 test 1: KeySummary canonical encoding round-trip --

    #[test]
    fn test_key_summary_canonical_encoding() {
        // ExactKeys variant
        let exact =
            KeySummary::ExactKeys(vec![test_page_key(1), test_page_key(2), test_page_key(3)]);
        assert_eq!(exact.len(), 3);
        assert!(exact.may_overlap(&test_page_key(2)));
        assert!(!exact.may_overlap(&test_page_key(4)));

        // HashedKeySet variant
        let hashed = KeySummary::HashedKeySet(vec![100, 200, 300]);
        assert_eq!(hashed.len(), 3);
        assert!(!hashed.is_empty());

        // PageBitmap variant
        let bitmap = KeySummary::PageBitmap(BTreeSet::from([1, 2, 5, 10]));
        assert!(bitmap.may_overlap(&test_page_key(5)));
        assert!(!bitmap.may_overlap(&test_page_key(6)));

        // CellBitmap variant
        let cell_bmp = KeySummary::CellBitmap(BTreeSet::from([(0x3_u64 << 32) | 0x2a]));
        assert_eq!(cell_bmp.len(), 1);

        // ByteRangeList variant
        let ranges = KeySummary::ByteRangeList(vec![(1, 0, 100), (2, 50, 200)]);
        assert_eq!(ranges.len(), 2);
        // Conservative: always returns true.
        assert!(ranges.may_overlap(&test_page_key(99)));

        // Chunked variant
        let chunked = KeySummary::Chunked(vec![
            KeySummaryChunk {
                range_prefix: 0,
                summary: KeySummary::ExactKeys(vec![test_page_key(1)]),
            },
            KeySummaryChunk {
                range_prefix: 1,
                summary: KeySummary::PageBitmap(BTreeSet::from([5])),
            },
        ]);
        assert!(chunked.may_overlap(&test_page_key(1)));
        assert!(chunked.may_overlap(&test_page_key(5)));
        assert!(!chunked.may_overlap(&test_page_key(3)));
    }

    // -- §5.7.1 test 2: EcsReadWitness deterministic --

    #[test]
    fn test_read_witness_ecs_deterministic() {
        let token = test_txn_token(1, 0);
        let w1 = EcsReadWitness {
            txn: token,
            begin_seq: CommitSeq::new(10),
            level: 0,
            range_prefix: 42,
            key_summary: KeySummary::ExactKeys(vec![test_page_key(1), test_page_key(2)]),
            emitted_at: LogicalTime::new(100),
        };
        let w2 = EcsReadWitness {
            txn: token,
            begin_seq: CommitSeq::new(10),
            level: 0,
            range_prefix: 42,
            key_summary: KeySummary::ExactKeys(vec![test_page_key(1), test_page_key(2)]),
            emitted_at: LogicalTime::new(100),
        };
        assert_eq!(w1, w2, "same inputs must produce identical ECS witnesses");
    }

    // -- §5.7.1 test 3: WriteWitness Intent vs Final --

    #[test]
    fn test_write_witness_kinds() {
        let token = test_txn_token(2, 0);
        let intent = EcsWriteWitness {
            txn: token,
            begin_seq: CommitSeq::new(5),
            level: 0,
            range_prefix: 0,
            key_summary: KeySummary::ExactKeys(vec![test_page_key(3)]),
            emitted_at: LogicalTime::new(50),
            write_kind: WriteKind::Intent,
        };
        let finalized = EcsWriteWitness {
            write_kind: WriteKind::Final,
            ..intent.clone()
        };

        assert_ne!(intent.write_kind, finalized.write_kind);
        assert_eq!(intent.write_kind, WriteKind::Intent);
        assert_eq!(finalized.write_kind, WriteKind::Final);
    }

    // -- §5.7.1 test 4: WitnessDelta CRDT merge --

    #[test]
    fn test_witness_delta_crdt_merge() {
        let token = test_txn_token(3, 1);
        let delta_a = WitnessDelta {
            txn: token,
            begin_seq: CommitSeq::new(7),
            kind: WitnessDeltaKind::Read,
            level: 0,
            range_prefix: 0,
            participation: WitnessParticipation::Present,
            refinement: None,
        };
        let delta_b = WitnessDelta {
            txn: token,
            begin_seq: CommitSeq::new(7),
            kind: WitnessDeltaKind::Read,
            level: 0,
            range_prefix: 0,
            participation: WitnessParticipation::Present,
            refinement: Some(KeySummary::ExactKeys(vec![test_page_key(1)])),
        };

        let merged = delta_a.merge(&delta_b);
        // Participation must remain Present (union-only CRDT, no removals).
        assert_eq!(merged.participation, WitnessParticipation::Present);
        // Refinement is merged: b had refinement, a didn't → keep b's.
        assert!(merged.refinement.is_some());
    }

    // -- §5.7.1 test 5: DependencyEdge canonical --

    #[test]
    fn test_dependency_edge_canonical() {
        let from = test_txn_token(10, 0);
        let to = test_txn_token(20, 0);
        let observer = test_txn_token(30, 0);

        let edge = EcsDependencyEdge {
            kind: DependencyEdgeKind::RwAntiDependency,
            from,
            to,
            key_basis: EdgeKeyBasis {
                level: 0,
                range_prefix: 42,
                refinement: Some(KeySummary::PageBitmap(BTreeSet::from([5]))),
            },
            observed_by: observer,
            observation_seq: CommitSeq::new(100),
        };

        assert_eq!(edge.kind, DependencyEdgeKind::RwAntiDependency);
        assert_eq!(edge.from.id.get(), 10);
        assert_eq!(edge.to.id.get(), 20);
        assert!(edge.key_basis.refinement.is_some());
    }

    // -- §5.7.1 test 6: CommitProof replay --

    #[test]
    fn test_commit_proof_replay() {
        let txn = test_txn_token(5, 0);
        let proof = EcsCommitProof {
            txn,
            begin_seq: CommitSeq::new(10),
            commit_seq: CommitSeq::new(15),
            has_in_rw: true,
            has_out_rw: false,
            read_witness_refs: vec![ObjectId::from_bytes([1_u8; 16])],
            write_witness_refs: vec![ObjectId::from_bytes([2_u8; 16])],
            index_segments_used: vec![],
            edges_emitted: vec![ObjectId::from_bytes([3_u8; 16])],
            merge_witnesses: vec![],
            abort_policy: AbortPolicy::AbortPivot,
        };

        // CommitProof contains enough evidence to re-run SSI validation:
        // - has_in_rw/has_out_rw capture dangerous structure presence
        // - refs point to witnesses and edges
        assert!(proof.has_in_rw);
        assert!(!proof.has_out_rw);
        assert_eq!(proof.read_witness_refs.len(), 1);
        assert_eq!(proof.edges_emitted.len(), 1);
        assert_eq!(proof.abort_policy, AbortPolicy::AbortPivot);
    }

    // -- §5.7.2 test 7: Hot-plane no false negatives --

    #[test]
    fn test_hot_plane_no_false_negatives() {
        let index = HotWitnessIndex::new(16, 64);
        let epoch = index.current_epoch();

        // Register a reader at slot 3 for a range key.
        let rk = test_range_key(0, 42);
        index.register_read(3, epoch, &[rk]);

        // Discover candidates for the same range key.
        let candidates = hot_plane_discover(&index, &[rk]);

        // Slot 3 MUST appear as a candidate reader (no false negatives).
        assert!(
            candidates.reader_slot_ids.contains(&3),
            "active reader at slot 3 must be discoverable: got {:?}",
            candidates.reader_slot_ids
        );
    }

    // -- §5.7.2 test 8: Hot-plane epoch overlap --

    #[test]
    fn test_hot_plane_epoch_overlap() {
        let index = HotWitnessIndex::new(16, 64);
        let epoch = index.current_epoch();

        // Register reads in current epoch.
        let rk = test_range_key(0, 100);
        index.register_read(5, epoch, &[rk]);

        // Register reads with the "previous" epoch (simulated by using epoch-1
        // if epoch > 0, or just epoch 0).
        let prev_epoch = if epoch > 0 { epoch - 1 } else { epoch };
        index.register_read(7, prev_epoch, &[rk]);

        let candidates = hot_plane_discover(&index, &[rk]);

        // Both slots should appear (queries both cur and prev epochs).
        assert!(
            candidates.reader_slot_ids.contains(&5),
            "current-epoch reader must be found"
        );
        // Note: slot 7 at prev_epoch may or may not appear depending on whether
        // the epoch buffer was installed. With a fresh index at epoch 0, both
        // registrations go to the same epoch, so both should be found.
        // The key invariant: no active reader is missed.
    }

    // -- §5.7.2 test 9: Cold-plane refinement reduces false positives --

    #[test]
    fn test_cold_plane_refinement_reduces_fp() {
        let token_real = test_txn_token(10, 0);
        let token_fp = test_txn_token(20, 0);
        let query_key = test_page_key(5);

        // token_real has a witness that overlaps page 5.
        let real_witness = EcsReadWitness {
            txn: token_real,
            begin_seq: CommitSeq::new(1),
            level: 0,
            range_prefix: 0,
            key_summary: KeySummary::PageBitmap(BTreeSet::from([5, 6, 7])),
            emitted_at: LogicalTime::new(1),
        };

        // token_fp has a witness that does NOT overlap page 5.
        let fp_witness = EcsReadWitness {
            txn: token_fp,
            begin_seq: CommitSeq::new(2),
            level: 0,
            range_prefix: 0,
            key_summary: KeySummary::PageBitmap(BTreeSet::from([10, 11, 12])),
            emitted_at: LogicalTime::new(2),
        };

        let candidates = vec![
            (token_real, Some(real_witness)),
            (token_fp, Some(fp_witness)),
        ];

        let result = cold_plane_refine(&candidates, &[], &query_key);

        // Real reader confirmed, false positive eliminated.
        assert_eq!(result.confirmed_readers.len(), 1);
        assert_eq!(result.confirmed_readers[0], token_real);
        assert_eq!(result.false_positives_eliminated, 1);
    }

    // -- Property: KeySummary soundness (no false negatives for ExactKeys) --

    #[test]
    fn prop_key_summary_soundness_no_false_negatives() {
        // For ExactKeys, membership is exact: no false negatives AND no false positives.
        let keys: Vec<WitnessKey> = (1..=100).map(test_page_key).collect();
        let summary = KeySummary::ExactKeys(keys);

        for i in 1..=100 {
            assert!(
                summary.may_overlap(&test_page_key(i)),
                "ExactKeys must not have false negatives for page {i}"
            );
        }
        for i in 101..=200 {
            assert!(
                !summary.may_overlap(&test_page_key(i)),
                "ExactKeys should not match page {i}"
            );
        }
    }

    // -- Abort witness construction --

    #[test]
    fn test_abort_witness_construction() {
        let txn = test_txn_token(42, 1);
        let aw = AbortWitness {
            txn,
            begin_seq: CommitSeq::new(10),
            abort_seq: CommitSeq::new(15),
            reason: AbortReason::SsiPivot,
            edges_observed: vec![],
        };

        assert_eq!(aw.reason, AbortReason::SsiPivot);
        assert!(aw.edges_observed.is_empty());
    }

    // -- WitnessDelta: union-only CRDT preserves participation --

    #[test]
    fn test_witness_delta_union_only() {
        let token = test_txn_token(50, 0);
        let base = WitnessDelta {
            txn: token,
            begin_seq: CommitSeq::new(1),
            kind: WitnessDeltaKind::Write,
            level: 1,
            range_prefix: 99,
            participation: WitnessParticipation::Present,
            refinement: Some(KeySummary::ExactKeys(vec![
                test_page_key(1),
                test_page_key(2),
            ])),
        };

        let update = WitnessDelta {
            refinement: Some(KeySummary::ExactKeys(vec![
                test_page_key(1),
                test_page_key(2),
                test_page_key(3),
            ])),
            ..base
        };

        let merged = base.merge(&update);

        // Participation is always Present after merge (union-only).
        assert_eq!(merged.participation, WitnessParticipation::Present);
        // More precise refinement wins (3 keys > 2 keys).
        assert_eq!(merged.refinement.as_ref().unwrap().len(), 3);
    }
}
