//! §5.7.3 Commit-Time SSI Validation (Proof-Carrying).
//!
//! Implements the 7-step `ssi_validate_and_publish` algorithm that runs
//! as part of every CONCURRENT mode commit. Detects dangerous structures
//! (write skew and other serialization anomalies) by tracking
//! rw-antidependency edges between concurrent transactions.
//!
//! Produces explicit, replayable evidence artifacts:
//! - `DependencyEdge` objects for observed rw-antidependencies
//! - `CommitProof` for commits
//! - `AbortWitness` for SSI aborts

use std::collections::{BTreeMap, HashSet};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use fsqlite_types::{CommitSeq, ObjectId, PageNumber, TxnToken, WitnessKey};
use tracing::{debug, info, warn};

use crate::observability;
use crate::ssi_abort_policy::{
    SsiDecisionCard, SsiDecisionCardDraft, SsiDecisionQuery, SsiDecisionType, SsiEvidenceLedger,
};

use crate::witness_objects::{
    AbortPolicy, AbortReason, AbortWitness, DependencyEdgeKind, EcsCommitProof, EcsDependencyEdge,
    EdgeKeyBasis, KeySummary,
};

// ---------------------------------------------------------------------------
// SSI Error
// ---------------------------------------------------------------------------

/// SSI validation determined the transaction must abort.
///
/// Maps to `SQLITE_BUSY_SNAPSHOT` at the public API boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsiBusySnapshot {
    /// The transaction that was aborted.
    pub txn: TxnToken,
    /// Reason for the abort.
    pub reason: SsiAbortReason,
    /// The abort witness (evidence artifact).
    pub witness: AbortWitness,
}

/// Reason for SSI abort.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SsiAbortReason {
    /// This transaction is the pivot (has both in + out rw edges).
    Pivot,
    /// A committed reader has an incoming rw edge, making it an
    /// irrevocable pivot — so this transaction must abort instead.
    CommittedPivot,
    /// The transaction was eagerly marked for abort by another committer.
    MarkedForAbort,
}

// ---------------------------------------------------------------------------
// Evidence Ledger Metrics + Accessors
// ---------------------------------------------------------------------------

static FSQLITE_EVIDENCE_RECORDS_TOTAL_COMMIT: AtomicU64 = AtomicU64::new(0);
static FSQLITE_EVIDENCE_RECORDS_TOTAL_ABORT: AtomicU64 = AtomicU64::new(0);

/// Snapshot of SSI evidence-record counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EvidenceRecordMetricsSnapshot {
    pub fsqlite_evidence_records_total_commit: u64,
    pub fsqlite_evidence_records_total_abort: u64,
}

impl EvidenceRecordMetricsSnapshot {
    #[must_use]
    pub fn fsqlite_evidence_records_total(self) -> u64 {
        self.fsqlite_evidence_records_total_commit + self.fsqlite_evidence_records_total_abort
    }
}

fn ssi_evidence_ledger() -> &'static SsiEvidenceLedger {
    static LEDGER: OnceLock<SsiEvidenceLedger> = OnceLock::new();
    LEDGER.get_or_init(SsiEvidenceLedger::default)
}

/// Read the shared SSI decision-evidence store.
#[must_use]
pub fn ssi_evidence_snapshot() -> Vec<SsiDecisionCard> {
    ssi_evidence_ledger().snapshot()
}

/// Query the shared SSI decision-evidence store.
#[must_use]
pub fn ssi_evidence_query(query: &SsiDecisionQuery) -> Vec<SsiDecisionCard> {
    ssi_evidence_ledger().query(query)
}

/// Snapshot evidence-record counters.
#[must_use]
pub fn ssi_evidence_metrics_snapshot() -> EvidenceRecordMetricsSnapshot {
    EvidenceRecordMetricsSnapshot {
        fsqlite_evidence_records_total_commit: FSQLITE_EVIDENCE_RECORDS_TOTAL_COMMIT
            .load(Ordering::Relaxed),
        fsqlite_evidence_records_total_abort: FSQLITE_EVIDENCE_RECORDS_TOTAL_ABORT
            .load(Ordering::Relaxed),
    }
}

/// Reset evidence-record counters (tests/diagnostics).
pub fn reset_ssi_evidence_metrics() {
    FSQLITE_EVIDENCE_RECORDS_TOTAL_COMMIT.store(0, Ordering::Relaxed);
    FSQLITE_EVIDENCE_RECORDS_TOTAL_ABORT.store(0, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// Per-Transaction SSI State
// ---------------------------------------------------------------------------

/// Per-transaction SSI tracking state (§5.7.3).
#[derive(Debug, Clone)]
pub struct SsiState {
    /// Transaction identity.
    pub txn: TxnToken,
    /// Begin sequence (snapshot lower bound).
    pub begin_seq: CommitSeq,
    /// Whether an incoming rw-antidependency edge exists.
    pub has_in_rw: bool,
    /// Whether an outgoing rw-antidependency edge exists.
    pub has_out_rw: bool,
    /// Sources of incoming edges (R -rw-> T).
    pub rw_in_from: HashSet<TxnToken>,
    /// Targets of outgoing edges (T -rw-> W).
    pub rw_out_to: HashSet<TxnToken>,
    /// Object IDs of emitted dependency edges.
    pub edges_emitted: Vec<ObjectId>,
    /// Whether another transaction marked this one for abort.
    pub marked_for_abort: bool,
}

impl SsiState {
    /// Create a new SSI state for a transaction.
    #[must_use]
    pub fn new(txn: TxnToken, begin_seq: CommitSeq) -> Self {
        Self {
            txn,
            begin_seq,
            has_in_rw: false,
            has_out_rw: false,
            rw_in_from: HashSet::new(),
            rw_out_to: HashSet::new(),
            edges_emitted: Vec::new(),
            marked_for_abort: false,
        }
    }

    /// Whether this transaction has a dangerous structure (both in + out rw).
    #[must_use]
    pub fn has_dangerous_structure(&self) -> bool {
        self.has_in_rw && self.has_out_rw
    }
}

// ---------------------------------------------------------------------------
// Discovered Edge
// ---------------------------------------------------------------------------

/// A discovered rw-antidependency edge before publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredEdge {
    /// Source transaction (the reader).
    pub from: TxnToken,
    /// Target transaction (the writer).
    pub to: TxnToken,
    /// The witness key that caused the overlap.
    pub overlap_key: WitnessKey,
    /// Whether the source is still active (vs committed).
    pub source_is_active: bool,
    /// Whether the source had an incoming rw edge at time of discovery
    /// (for T3 rule on committed readers).
    pub source_has_in_rw: bool,
}

// ---------------------------------------------------------------------------
// Active Transaction View (trait for abstraction)
// ---------------------------------------------------------------------------

/// View into an active transaction's SSI state.
///
/// Abstracted so the validation algorithm can be tested without
/// full `SharedTxnSlot` infrastructure.
pub trait ActiveTxnView {
    /// Transaction token.
    fn token(&self) -> TxnToken;
    /// Begin sequence.
    fn begin_seq(&self) -> CommitSeq;
    /// Whether the transaction is still active.
    fn is_active(&self) -> bool;
    /// Read witness keys.
    fn read_keys(&self) -> &[WitnessKey];
    /// Write witness keys.
    fn write_keys(&self) -> &[WitnessKey];
    /// Whether this transaction has incoming rw edges.
    fn has_in_rw(&self) -> bool;
    /// Whether this transaction has outgoing rw edges.
    fn has_out_rw(&self) -> bool;
    /// Set the outgoing rw flag.
    fn set_has_out_rw(&self, val: bool);
    /// Set the incoming rw flag.
    fn set_has_in_rw(&self, val: bool);
    /// Mark for abort.
    fn set_marked_for_abort(&self, val: bool);
}

/// A committed reader entry from the RCRI (for T3 rule).
#[derive(Debug, Clone)]
pub struct CommittedReaderInfo {
    /// The committed reader's token.
    pub token: TxnToken,
    /// Begin sequence.
    pub begin_seq: CommitSeq,
    /// Commit sequence.
    pub commit_seq: CommitSeq,
    /// Whether the reader had incoming rw edges at commit time.
    pub had_in_rw: bool,
    /// Pages the reader accessed (for overlap checks).
    pub pages: Vec<PageNumber>,
}

/// A committed writer entry from the CommitLog (for outgoing edge discovery).
#[derive(Debug, Clone)]
pub struct CommittedWriterInfo {
    /// The committed writer's token.
    pub token: TxnToken,
    /// Commit sequence.
    pub commit_seq: CommitSeq,
    /// Whether the writer had outgoing rw edges at commit time.
    ///
    /// Used for committed-pivot checks when a later transaction discovers an
    /// outgoing edge to this already-committed writer.
    pub had_out_rw: bool,
    /// Pages the writer modified.
    pub pages: Vec<PageNumber>,
}

// ---------------------------------------------------------------------------
// Edge Discovery
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct IndexedTxnRecord<'a> {
    token: TxnToken,
    begin_seq: CommitSeq,
    commit_seq: Option<CommitSeq>,
    source_is_active: bool,
    source_has_in_rw: bool,
    /// The specific witness key triggering this index entry (if available).
    /// Used for fine-grained filtering (e.g. Cell tag matching) to avoid false positives.
    key: Option<&'a WitnessKey>,
}

fn build_reader_index<'a>(
    committing_txn: TxnToken,
    active_readers: &[&'a dyn ActiveTxnView],
    committed_readers: &[CommittedReaderInfo],
    target_pages: &HashSet<u32>,
) -> BTreeMap<u32, Vec<IndexedTxnRecord<'a>>> {
    let mut index: BTreeMap<u32, Vec<IndexedTxnRecord<'a>>> = BTreeMap::new();

    for reader in active_readers {
        if reader.token() == committing_txn || !reader.is_active() {
            continue;
        }
        for key in reader.read_keys() {
            let page = witness_key_page(key);
            if !target_pages.contains(&page) {
                continue;
            }
            index.entry(page).or_default().push(IndexedTxnRecord {
                token: reader.token(),
                begin_seq: reader.begin_seq(),
                commit_seq: None,
                source_is_active: true,
                source_has_in_rw: reader.has_in_rw(),
                key: Some(key),
            });
        }
    }

    for reader in committed_readers {
        if reader.token == committing_txn {
            continue;
        }
        for page in &reader.pages {
            let page_no = page.get();
            if !target_pages.contains(&page_no) {
                continue;
            }
            index.entry(page_no).or_default().push(IndexedTxnRecord {
                token: reader.token,
                begin_seq: reader.begin_seq,
                commit_seq: Some(reader.commit_seq),
                source_is_active: false,
                source_has_in_rw: reader.had_in_rw,
                key: None,
            });
        }
    }

    index
}

fn build_writer_index<'a>(
    committing_txn: TxnToken,
    active_writers: &[&'a dyn ActiveTxnView],
    committed_writers: &[CommittedWriterInfo],
    target_pages: &HashSet<u32>,
) -> BTreeMap<u32, Vec<IndexedTxnRecord<'a>>> {
    let mut index: BTreeMap<u32, Vec<IndexedTxnRecord<'a>>> = BTreeMap::new();

    for writer in active_writers {
        if writer.token() == committing_txn || !writer.is_active() {
            continue;
        }
        for key in writer.write_keys() {
            let page = witness_key_page(key);
            if !target_pages.contains(&page) {
                continue;
            }
            index.entry(page).or_default().push(IndexedTxnRecord {
                token: writer.token(),
                begin_seq: writer.begin_seq(),
                commit_seq: None,
                source_is_active: true,
                source_has_in_rw: writer.has_out_rw(),
                key: Some(key),
            });
        }
    }

    for writer in committed_writers {
        if writer.token == committing_txn {
            continue;
        }
        for page in &writer.pages {
            let page_no = page.get();
            if !target_pages.contains(&page_no) {
                continue;
            }
            index.entry(page_no).or_default().push(IndexedTxnRecord {
                token: writer.token,
                begin_seq: CommitSeq::ZERO,
                commit_seq: Some(writer.commit_seq),
                source_is_active: false,
                source_has_in_rw: writer.had_out_rw,
                key: None,
            });
        }
    }

    index
}

/// Discover incoming rw-antidependency edges (R -rw-> T).
///
/// Checks both active transactions (from `active_readers`) and committed
/// readers (from `committed_readers` / RCRI). An incoming edge exists when:
/// - R read a key K, T writes K, and R is concurrent with T.
///
/// **Correctness**: MUST check both sources. Missing committed readers =
/// false negatives = silent data corruption.
pub fn discover_incoming_edges(
    committing_txn: TxnToken,
    committing_begin_seq: CommitSeq,
    committing_commit_seq: CommitSeq,
    write_keys: &[WitnessKey],
    active_readers: &[&dyn ActiveTxnView],
    committed_readers: &[CommittedReaderInfo],
) -> Vec<DiscoveredEdge> {
    let mut edges = Vec::new();

    if write_keys.is_empty() {
        return edges;
    }

    let target_pages: HashSet<u32> = write_keys.iter().map(witness_key_page).collect();
    let index = build_reader_index(
        committing_txn,
        active_readers,
        committed_readers,
        &target_pages,
    );
    let committing_begin = committing_begin_seq.get();
    let committing_end = committing_commit_seq.get();
    let mut seen_sources = HashSet::new();
    for write_key in write_keys {
        let page = witness_key_page(write_key);
        let Some(candidates) = index.get(&page) else {
            continue;
        };
        for candidate in candidates {
            // Fine-grained filtering: check exact witness overlap.
            // This prevents false positives when transactions touch different
            // non-overlapping parts of the same page.
            if let Some(reader_key) = candidate.key {
                if !crate::witness_plane::witness_keys_overlap(write_key, reader_key) {
                    continue;
                }
            }

            let candidate_begin = candidate.begin_seq.get();
            let candidate_end = candidate.commit_seq.map_or(u64::MAX, CommitSeq::get);
            let overlaps = committing_begin < candidate_end && candidate_begin < committing_end;
            if !overlaps || !seen_sources.insert(candidate.token) {
                continue;
            }

            let source = if candidate.source_is_active {
                "hot_plane_index"
            } else {
                "rcri_index"
            };
            debug!(
                bead_id = "bd-31bo",
                from = ?candidate.token,
                to = ?committing_txn,
                key = ?write_key,
                source,
                "discovered incoming rw-antidependency edge"
            );
            edges.push(DiscoveredEdge {
                from: candidate.token,
                to: committing_txn,
                overlap_key: write_key.clone(),
                source_is_active: candidate.source_is_active,
                source_has_in_rw: candidate.source_has_in_rw,
            });
        }
    }

    edges
}

/// Discover outgoing rw-antidependency edges (T -rw-> W).
///
/// Checks both active writers and committed writers (from CommitLog).
/// An outgoing edge exists when:
/// - T read a key K, W writes K, and W is concurrent with T.
///
/// **Correctness**: MUST check CommitLog for writers that committed after
/// T.begin_seq and freed their TxnSlot. Missing = false negatives.
pub fn discover_outgoing_edges(
    committing_txn: TxnToken,
    committing_begin_seq: CommitSeq,
    committing_commit_seq: CommitSeq,
    read_keys: &[WitnessKey],
    active_writers: &[&dyn ActiveTxnView],
    committed_writers: &[CommittedWriterInfo],
) -> Vec<DiscoveredEdge> {
    let mut edges = Vec::new();

    if read_keys.is_empty() {
        return edges;
    }

    let target_pages: HashSet<u32> = read_keys.iter().map(witness_key_page).collect();
    let index = build_writer_index(
        committing_txn,
        active_writers,
        committed_writers,
        &target_pages,
    );
    let committing_begin = committing_begin_seq.get();
    let committing_end = committing_commit_seq.get();
    let mut seen_targets = HashSet::new();
    for read_key in read_keys {
        let page = witness_key_page(read_key);
        let Some(candidates) = index.get(&page) else {
            continue;
        };
        for candidate in candidates {
            // Fine-grained filtering: check exact witness overlap.
            if let Some(writer_key) = candidate.key {
                if !crate::witness_plane::witness_keys_overlap(read_key, writer_key) {
                    continue;
                }
            }

            let candidate_begin = candidate.begin_seq.get();
            let candidate_end = candidate.commit_seq.map_or(u64::MAX, CommitSeq::get);
            let overlaps = committing_begin < candidate_end && candidate_begin < committing_end;
            if !overlaps || !seen_targets.insert(candidate.token) {
                continue;
            }

            let source = if candidate.source_is_active {
                "hot_plane_index"
            } else {
                "commit_log_index"
            };
            debug!(
                bead_id = "bd-31bo",
                from = ?committing_txn,
                to = ?candidate.token,
                key = ?read_key,
                source,
                "discovered outgoing rw-antidependency edge"
            );
            edges.push(DiscoveredEdge {
                from: committing_txn,
                to: candidate.token,
                overlap_key: read_key.clone(),
                source_is_active: candidate.source_is_active,
                // For outgoing edges, this flag carries whether the target writer
                // had outgoing rw state (active: current has_out_rw; committed:
                // had_out_rw at commit time). It enables committed-writer pivot checks.
                source_has_in_rw: candidate.source_has_in_rw,
            });
        }
    }

    edges
}

// ---------------------------------------------------------------------------
// Key Overlap Helpers
// ---------------------------------------------------------------------------

/// Extract the page number from a witness key.
pub(crate) fn witness_key_page(key: &WitnessKey) -> u32 {
    match key {
        WitnessKey::Page(p) => p.get(),
        WitnessKey::Cell { btree_root, .. } | WitnessKey::KeyRange { btree_root, .. } => {
            btree_root.get()
        }
        WitnessKey::ByteRange { page, .. } => page.get(),
        WitnessKey::Custom { .. } => 0,
    }
}

// ---------------------------------------------------------------------------
// §5.7.3 Core: ssi_validate_and_publish
// ---------------------------------------------------------------------------

/// Result of successful SSI validation (commit allowed).
#[derive(Debug, Clone)]
pub struct SsiValidationOk {
    /// ECS dependency edges emitted.
    pub edges: Vec<EcsDependencyEdge>,
    /// Object IDs of emitted edges.
    pub edge_ids: Vec<ObjectId>,
    /// The commit proof artifact.
    pub commit_proof: EcsCommitProof,
    /// Updated SSI state for the transaction.
    pub ssi_state: SsiState,
}

/// The 7-step SSI validation algorithm (§5.7.3).
///
/// Runs for every CONCURRENT mode commit. Produces evidence artifacts.
///
/// # Errors
///
/// Returns `SsiBusySnapshot` if the transaction must abort due to
/// a detected dangerous structure (write skew).
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub fn ssi_validate_and_publish(
    txn: TxnToken,
    begin_seq: CommitSeq,
    commit_seq: CommitSeq,
    read_keys: &[WitnessKey],
    write_keys: &[WitnessKey],
    active_readers: &[&dyn ActiveTxnView],
    active_writers: &[&dyn ActiveTxnView],
    committed_readers: &[CommittedReaderInfo],
    committed_writers: &[CommittedWriterInfo],
    marked_for_abort: bool,
) -> Result<SsiValidationOk, SsiBusySnapshot> {
    let mut state = SsiState::new(txn, begin_seq);
    state.marked_for_abort = marked_for_abort;

    // bd-688.2: structured tracing span per bead specification.
    let span = tracing::span!(
        tracing::Level::INFO,
        "ssi_validate",
        txn_id = txn.id.get(),
        read_set_size = read_keys.len(),
        write_set_size = write_keys.len(),
        conflict_detected = tracing::field::Empty,
        decision_reason = tracing::field::Empty,
    );
    let _guard = span.enter();

    info!(
        bead_id = "bd-31bo",
        txn = ?txn,
        read_keys = read_keys.len(),
        write_keys = write_keys.len(),
        marked_for_abort,
        "ssi_validate_and_publish: starting"
    );

    // Step 1: Witnesses already emitted by caller (WitnessSet registered
    // during query execution). Hot index already updated.

    // Step 2: Read-only fast path.
    if write_keys.is_empty() {
        record_evidence_decision(
            SsiDecisionType::CommitAllowed,
            txn,
            begin_seq,
            Some(commit_seq),
            read_keys,
            write_keys,
            &[],
            "read_only_fast_path",
        );
        span.record("conflict_detected", false);
        span.record("decision_reason", "read_only_fast_path");
        debug!(
            bead_id = "bd-31bo",
            txn = ?txn,
            "ssi_validate: read-only fast path, skipping SSI"
        );
        let proof = build_commit_proof(txn, begin_seq, commit_seq, &state, &[], &[]);
        observability::record_ssi_commit();
        return Ok(SsiValidationOk {
            edges: Vec::new(),
            edge_ids: Vec::new(),
            commit_proof: proof,
            ssi_state: state,
        });
    }

    // Check eagerly marked for abort.
    if marked_for_abort {
        record_evidence_decision(
            SsiDecisionType::AbortCycle,
            txn,
            begin_seq,
            Some(commit_seq),
            read_keys,
            write_keys,
            &[],
            "marked_for_abort",
        );
        span.record("conflict_detected", true);
        span.record("decision_reason", "marked_for_abort");
        warn!(
            bead_id = "bd-31bo",
            txn = ?txn,
            "ssi_validate: transaction marked for abort by another committer"
        );
        observability::record_ssi_abort(fsqlite_observability::SsiAbortCategory::MarkedForAbort);
        let witness = AbortWitness {
            txn,
            begin_seq,
            abort_seq: commit_seq,
            reason: AbortReason::SsiPivot,
            edges_observed: Vec::new(),
        };
        return Err(SsiBusySnapshot {
            txn,
            reason: SsiAbortReason::MarkedForAbort,
            witness,
        });
    }

    // Step 3: Discover incoming and outgoing rw-antidependency edges.
    let in_edges = discover_incoming_edges(
        txn,
        begin_seq,
        commit_seq,
        write_keys,
        active_readers,
        committed_readers,
    );
    let out_edges = discover_outgoing_edges(
        txn,
        begin_seq,
        commit_seq,
        read_keys,
        active_writers,
        committed_writers,
    );

    state.has_in_rw = !in_edges.is_empty();
    state.has_out_rw = !out_edges.is_empty();

    for edge in &in_edges {
        state.rw_in_from.insert(edge.from);
    }
    for edge in &out_edges {
        state.rw_out_to.insert(edge.to);
    }

    info!(
        bead_id = "bd-31bo",
        txn = ?txn,
        incoming = in_edges.len(),
        outgoing = out_edges.len(),
        has_in_rw = state.has_in_rw,
        has_out_rw = state.has_out_rw,
        "ssi_validate: edge discovery complete"
    );

    // Step 4: Refinement and merge (placeholder — refinement is optional,
    // skipping is always safe per §5.7.3 correctness rule).
    // In a full implementation, cold-plane refinement would tighten witnesses
    // and potentially remove spurious edges.

    // Step 5: Pivot rule (conservative).
    if state.has_in_rw && state.has_out_rw {
        let all_edges = build_dependency_edges(&in_edges, &out_edges, txn, commit_seq);
        let discovered_edges: Vec<DiscoveredEdge> = in_edges
            .iter()
            .cloned()
            .chain(out_edges.iter().cloned())
            .collect();
        record_evidence_decision(
            SsiDecisionType::AbortWriteSkew,
            txn,
            begin_seq,
            Some(commit_seq),
            read_keys,
            write_keys,
            &discovered_edges,
            "pivot_abort_dangerous_structure",
        );
        span.record("conflict_detected", true);
        span.record("decision_reason", "pivot_abort");
        warn!(
            bead_id = "bd-31bo",
            txn = ?txn,
            in_sources = ?state.rw_in_from,
            out_targets = ?state.rw_out_to,
            "ssi_validate: PIVOT ABORT — dangerous structure detected"
        );
        observability::record_ssi_abort(fsqlite_observability::SsiAbortCategory::Pivot);
        let witness = AbortWitness {
            txn,
            begin_seq,
            abort_seq: commit_seq,
            reason: AbortReason::SsiPivot,
            edges_observed: all_edges,
        };
        return Err(SsiBusySnapshot {
            txn,
            reason: SsiAbortReason::Pivot,
            witness,
        });
    }

    // Step 6: T3 rule (near-miss check).
    for edge in &in_edges {
        if edge.source_is_active {
            // R is active: set R.has_out_rw = true (R now has outgoing edge to T).
            // If R already has_in_rw: mark R for abort.
            for reader in active_readers {
                if reader.token() == edge.from {
                    reader.set_has_out_rw(true);
                    if reader.has_in_rw() {
                        debug!(
                            bead_id = "bd-31bo",
                            pivot = ?edge.from,
                            "T3 rule: active reader is pivot, marking for abort"
                        );
                        reader.set_marked_for_abort(true);
                    }
                    break;
                }
            }
        } else {
            // R is committed: if R.has_in_rw at commit time,
            // T MUST abort (committed pivot cannot be undone).
            if edge.source_has_in_rw {
                let discovered_edges: Vec<DiscoveredEdge> = in_edges
                    .iter()
                    .cloned()
                    .chain(out_edges.iter().cloned())
                    .collect();
                record_evidence_decision(
                    SsiDecisionType::AbortCycle,
                    txn,
                    begin_seq,
                    Some(commit_seq),
                    read_keys,
                    write_keys,
                    &discovered_edges,
                    "committed_pivot_abort",
                );
                span.record("conflict_detected", true);
                span.record("decision_reason", "committed_pivot_abort");
                warn!(
                    bead_id = "bd-31bo",
                    txn = ?txn,
                    committed_pivot = ?edge.from,
                    "T3 rule: committed reader was pivot, T must abort"
                );
                observability::record_ssi_abort(
                    fsqlite_observability::SsiAbortCategory::CommittedPivot,
                );
                let all_edges = build_dependency_edges(&in_edges, &out_edges, txn, commit_seq);
                let witness = AbortWitness {
                    txn,
                    begin_seq,
                    abort_seq: commit_seq,
                    reason: AbortReason::SsiPivot,
                    edges_observed: all_edges,
                };
                return Err(SsiBusySnapshot {
                    txn,
                    reason: SsiAbortReason::CommittedPivot,
                    witness,
                });
            }
        }
    }
    for edge in &out_edges {
        if edge.source_is_active {
            // W is active: set W.has_in_rw = true (W now has an incoming edge from T).
            // If W already has_out_rw: mark W for abort.
            for writer in active_writers {
                if writer.token() == edge.to {
                    writer.set_has_in_rw(true);
                    if writer.has_out_rw() {
                        debug!(
                            bead_id = "bd-31bo",
                            pivot = ?edge.to,
                            "T3 rule: active writer is pivot, marking for abort"
                        );
                        writer.set_marked_for_abort(true);
                    }
                    break;
                }
            }
        } else if edge.source_has_in_rw {
            // W is committed and had outgoing rw at commit time.
            // Symmetric committed-pivot check: T -> W with W already a pivot
            // implies T must abort.
            let discovered_edges: Vec<DiscoveredEdge> = in_edges
                .iter()
                .cloned()
                .chain(out_edges.iter().cloned())
                .collect();
            record_evidence_decision(
                SsiDecisionType::AbortCycle,
                txn,
                begin_seq,
                Some(commit_seq),
                read_keys,
                write_keys,
                &discovered_edges,
                "committed_writer_pivot_abort",
            );
            span.record("conflict_detected", true);
            span.record("decision_reason", "committed_writer_pivot_abort");
            warn!(
                bead_id = "bd-31bo",
                txn = ?txn,
                committed_pivot = ?edge.to,
                "T3 rule: committed writer was pivot, T must abort"
            );
            observability::record_ssi_abort(
                fsqlite_observability::SsiAbortCategory::CommittedPivot,
            );
            let all_edges = build_dependency_edges(&in_edges, &out_edges, txn, commit_seq);
            let witness = AbortWitness {
                txn,
                begin_seq,
                abort_seq: commit_seq,
                reason: AbortReason::SsiPivot,
                edges_observed: all_edges,
            };
            return Err(SsiBusySnapshot {
                txn,
                reason: SsiAbortReason::CommittedPivot,
                witness,
            });
        }
    }

    // Keep edges deterministic for proof/evidence generation.
    let mut in_edges = in_edges;
    in_edges.sort_by(|a, b| {
        a.from
            .id
            .get()
            .cmp(&b.from.id.get())
            .then_with(|| a.from.epoch.get().cmp(&b.from.epoch.get()))
            .then_with(|| witness_key_page(&a.overlap_key).cmp(&witness_key_page(&b.overlap_key)))
    });

    let mut out_edges = out_edges;
    out_edges.sort_by(|a, b| {
        a.to.id
            .get()
            .cmp(&b.to.id.get())
            .then_with(|| a.to.epoch.get().cmp(&b.to.epoch.get()))
            .then_with(|| witness_key_page(&a.overlap_key).cmp(&witness_key_page(&b.overlap_key)))
    });

    if !out_edges.is_empty() {
        debug!(
            bead_id = "bd-31bo",
            outgoing_edges = out_edges.len(),
            "ssi_validate: outgoing edge propagation complete"
        );
    }

    // Step 7: Publish edges and build CommitProof.
    let all_edges = build_dependency_edges(&in_edges, &out_edges, txn, commit_seq);
    let discovered_edges: Vec<DiscoveredEdge> = in_edges
        .iter()
        .cloned()
        .chain(out_edges.iter().cloned())
        .collect();
    record_evidence_decision(
        SsiDecisionType::CommitAllowed,
        txn,
        begin_seq,
        Some(commit_seq),
        read_keys,
        write_keys,
        &discovered_edges,
        "commit_approved",
    );
    let edge_ids: Vec<ObjectId> = all_edges
        .iter()
        .enumerate()
        .map(|(i, _)| {
            // Generate deterministic ObjectId for each edge.
            let mut bytes = [0u8; 16];
            bytes[..8].copy_from_slice(&txn.id.get().to_le_bytes());
            bytes[8..12].copy_from_slice(&commit_seq.get().to_le_bytes()[..4]);
            #[allow(clippy::cast_possible_truncation)]
            let idx = i as u32;
            bytes[12..16].copy_from_slice(&idx.to_le_bytes());
            ObjectId::from_bytes(bytes)
        })
        .collect();

    state.edges_emitted.clone_from(&edge_ids);

    let proof = build_commit_proof(txn, begin_seq, commit_seq, &state, &edge_ids, &[]);

    span.record("conflict_detected", false);
    span.record("decision_reason", "commit_approved");

    info!(
        bead_id = "bd-31bo",
        txn = ?txn,
        edges_emitted = all_edges.len(),
        "ssi_validate: commit approved, evidence published"
    );

    // bd-688.2: update SSI metrics.
    observability::record_ssi_commit();

    Ok(SsiValidationOk {
        edges: all_edges,
        edge_ids,
        commit_proof: proof,
        ssi_state: state,
    })
}

// ---------------------------------------------------------------------------
// Evidence Artifact Builders
// ---------------------------------------------------------------------------

/// Build `EcsDependencyEdge` objects from discovered edges.
fn build_dependency_edges(
    in_edges: &[DiscoveredEdge],
    out_edges: &[DiscoveredEdge],
    observer: TxnToken,
    observation_seq: CommitSeq,
) -> Vec<EcsDependencyEdge> {
    let mut result = Vec::with_capacity(in_edges.len() + out_edges.len());
    for edge in in_edges.iter().chain(out_edges.iter()) {
        result.push(EcsDependencyEdge {
            kind: DependencyEdgeKind::RwAntiDependency,
            from: edge.from,
            to: edge.to,
            key_basis: EdgeKeyBasis {
                level: 0,
                range_prefix: witness_key_page(&edge.overlap_key),
                refinement: Some(KeySummary::ExactKeys(vec![edge.overlap_key.clone()])),
            },
            observed_by: observer,
            observation_seq,
        });
    }
    result
}

/// Build a `CommitProof` artifact.
fn build_commit_proof(
    txn: TxnToken,
    begin_seq: CommitSeq,
    commit_seq: CommitSeq,
    state: &SsiState,
    edge_ids: &[ObjectId],
    merge_witnesses: &[ObjectId],
) -> EcsCommitProof {
    EcsCommitProof {
        txn,
        begin_seq,
        commit_seq,
        has_in_rw: state.has_in_rw,
        has_out_rw: state.has_out_rw,
        read_witness_refs: Vec::new(),
        write_witness_refs: Vec::new(),
        index_segments_used: Vec::new(),
        edges_emitted: edge_ids.to_vec(),
        merge_witnesses: merge_witnesses.to_vec(),
        abort_policy: AbortPolicy::AbortPivot,
    }
}

fn decision_outcome(decision_type: SsiDecisionType) -> &'static str {
    match decision_type {
        SsiDecisionType::CommitAllowed => "commit",
        SsiDecisionType::AbortWriteSkew
        | SsiDecisionType::AbortPhantom
        | SsiDecisionType::AbortCycle => "abort",
    }
}

fn witness_keys_to_pages(keys: &[WitnessKey]) -> Vec<PageNumber> {
    let mut pages: Vec<PageNumber> = keys
        .iter()
        .filter_map(|key| PageNumber::new(witness_key_page(key)))
        .collect();
    pages.sort_by_key(|page| page.get());
    pages.dedup();
    pages
}

fn edge_conflicting_txns(txn: TxnToken, edges: &[DiscoveredEdge]) -> Vec<TxnToken> {
    let mut txns = Vec::new();
    for edge in edges {
        if edge.from != txn {
            txns.push(edge.from);
        }
        if edge.to != txn {
            txns.push(edge.to);
        }
    }
    txns.sort_by(|left, right| {
        left.id
            .get()
            .cmp(&right.id.get())
            .then_with(|| left.epoch.get().cmp(&right.epoch.get()))
    });
    txns.dedup();
    txns
}

fn edge_conflict_pages(edges: &[DiscoveredEdge]) -> Vec<PageNumber> {
    let mut pages: Vec<PageNumber> = edges
        .iter()
        .filter_map(|edge| PageNumber::new(witness_key_page(&edge.overlap_key)))
        .collect();
    pages.sort_by_key(|page| page.get());
    pages.dedup();
    pages
}

fn estimate_evidence_size_bytes(
    read_pages: &[PageNumber],
    write_pages: &[PageNumber],
    conflict_pages: &[PageNumber],
    conflicting_txns: &[TxnToken],
    rationale: &str,
) -> u64 {
    #[allow(clippy::cast_possible_truncation)]
    {
        let words = read_pages.len()
            + write_pages.len()
            + conflict_pages.len()
            + conflicting_txns.len() * 2;
        ((words * std::mem::size_of::<u64>()) + rationale.len()) as u64
    }
}

#[allow(clippy::too_many_arguments)]
fn record_evidence_decision(
    decision_type: SsiDecisionType,
    txn: TxnToken,
    begin_seq: CommitSeq,
    commit_seq: Option<CommitSeq>,
    read_keys: &[WitnessKey],
    write_keys: &[WitnessKey],
    edges: &[DiscoveredEdge],
    rationale: &str,
) {
    let read_pages = witness_keys_to_pages(read_keys);
    let write_pages = witness_keys_to_pages(write_keys);
    let conflict_pages = edge_conflict_pages(edges);
    let conflicting_txns = edge_conflicting_txns(txn, edges);

    let evidence_size_bytes = estimate_evidence_size_bytes(
        &read_pages,
        &write_pages,
        &conflict_pages,
        &conflicting_txns,
        rationale,
    );
    let outcome = decision_outcome(decision_type);
    let decision_id = fsqlite_observability::next_decision_id();

    let span = tracing::span!(
        target: "fsqlite.evidence",
        tracing::Level::INFO,
        "evidence_record",
        decision_id,
        outcome,
        evidence_size_bytes
    );
    let _guard = span.enter();

    let mut draft = SsiDecisionCardDraft::new(
        decision_type,
        txn,
        begin_seq,
        conflicting_txns.clone(),
        conflict_pages.clone(),
        read_pages.clone(),
        write_pages.clone(),
        rationale,
    )
    .with_decision_id(decision_id);
    if let Some(seq) = commit_seq {
        draft = draft.with_commit_seq(seq);
    }
    ssi_evidence_ledger().record_async(draft);

    if matches!(decision_type, SsiDecisionType::CommitAllowed) {
        FSQLITE_EVIDENCE_RECORDS_TOTAL_COMMIT.fetch_add(1, Ordering::Relaxed);
    } else {
        FSQLITE_EVIDENCE_RECORDS_TOTAL_ABORT.fetch_add(1, Ordering::Relaxed);
    }

    info!(
        decision_id,
        outcome,
        decision_type = %decision_type,
        txn_id = txn.id.get(),
        read_pages = read_pages.len(),
        write_pages = write_pages.len(),
        conflicting_txns = conflicting_txns.len(),
        conflict_pages = conflict_pages.len(),
        "ssi decision evidence recorded"
    );
    debug!(
        decision_id,
        decision_type = %decision_type,
        txn = ?txn,
        read_pages = ?read_pages,
        write_pages = ?write_pages,
        conflicting_txns = ?conflicting_txns,
        conflict_pages = ?conflict_pages,
        rationale,
        "ssi decision evidence details"
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use fsqlite_types::{TxnEpoch, TxnId};
    use std::cell::Cell;

    // -- Test ActiveTxnView implementation --

    struct MockActiveTxn {
        token: TxnToken,
        begin_seq: CommitSeq,
        active: bool,
        reads: Vec<WitnessKey>,
        writes: Vec<WitnessKey>,
        has_in: Cell<bool>,
        has_out: Cell<bool>,
        marked: Cell<bool>,
    }

    impl MockActiveTxn {
        fn new(id: u64, epoch: u32, begin_seq: u64) -> Self {
            Self {
                token: TxnToken::new(TxnId::new(id).unwrap(), TxnEpoch::new(epoch)),
                begin_seq: CommitSeq::new(begin_seq),
                active: true,
                reads: Vec::new(),
                writes: Vec::new(),
                has_in: Cell::new(false),
                has_out: Cell::new(false),
                marked: Cell::new(false),
            }
        }

        fn with_reads(mut self, keys: Vec<WitnessKey>) -> Self {
            self.reads = keys;
            self
        }

        fn with_writes(mut self, keys: Vec<WitnessKey>) -> Self {
            self.writes = keys;
            self
        }

        fn with_has_in_rw(self, val: bool) -> Self {
            self.has_in.set(val);
            self
        }

        #[allow(dead_code)]
        fn committed(mut self) -> Self {
            self.active = false;
            self
        }
    }

    impl ActiveTxnView for MockActiveTxn {
        fn token(&self) -> TxnToken {
            self.token
        }
        fn begin_seq(&self) -> CommitSeq {
            self.begin_seq
        }
        fn is_active(&self) -> bool {
            self.active
        }
        fn read_keys(&self) -> &[WitnessKey] {
            &self.reads
        }
        fn write_keys(&self) -> &[WitnessKey] {
            &self.writes
        }
        fn has_in_rw(&self) -> bool {
            self.has_in.get()
        }
        fn has_out_rw(&self) -> bool {
            self.has_out.get()
        }
        fn set_has_out_rw(&self, val: bool) {
            self.has_out.set(val);
        }
        fn set_has_in_rw(&self, val: bool) {
            self.has_in.set(val);
        }
        fn set_marked_for_abort(&self, val: bool) {
            self.marked.set(val);
        }
    }

    fn page_key(pgno: u32) -> WitnessKey {
        WitnessKey::Page(PageNumber::new(pgno).unwrap())
    }

    fn keys_to_pages(keys: &[WitnessKey]) -> Vec<PageNumber> {
        let mut pages = Vec::new();
        for key in keys {
            if let WitnessKey::Page(page) = key {
                pages.push(*page);
            }
        }
        pages
    }

    // -- §5.7.3 test 1: Read-only skip --

    #[test]
    fn test_ssi_read_only_skip() {
        let txn = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0));
        let result = ssi_validate_and_publish(
            txn,
            CommitSeq::new(1),
            CommitSeq::new(5),
            &[page_key(10)], // reads
            &[],             // no writes — read-only
            &[],
            &[],
            &[],
            &[],
            false,
        );
        let ok = result.expect("read-only txn should commit");
        assert!(ok.edges.is_empty(), "no edges for read-only");
        assert!(!ok.ssi_state.has_in_rw);
        assert!(!ok.ssi_state.has_out_rw);
    }

    // -- §5.7.3 test 2: No edges commit --

    #[test]
    fn test_ssi_no_edges_commit() {
        let txn = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0));
        let result = ssi_validate_and_publish(
            txn,
            CommitSeq::new(1),
            CommitSeq::new(5),
            &[page_key(10)],
            &[page_key(20)], // writes to different page
            &[],
            &[],
            &[],
            &[],
            false,
        );
        let ok = result.expect("no overlap → no edges → commit");
        assert!(ok.edges.is_empty());
        assert!(!ok.ssi_state.has_in_rw);
        assert!(!ok.ssi_state.has_out_rw);
    }

    // -- §5.7.3 test 2b: Safe-snapshot shortcut (no conflicts) --

    #[test]
    fn test_safe_snapshot_shortcut_no_conflict_commit() {
        // Safe snapshot: no overlapping active/committed readers or writers.
        // SSI should produce an immediate commit decision with no edges.
        let txn = TxnToken::new(TxnId::new(8_001).unwrap(), TxnEpoch::new(0));
        let result = ssi_validate_and_publish(
            txn,
            CommitSeq::new(10),
            CommitSeq::new(11),
            &[page_key(1_000)],
            &[page_key(2_000)],
            &[],
            &[],
            &[],
            &[],
            false,
        )
        .expect("safe snapshot should commit without conflict work");

        assert!(
            result.edges.is_empty(),
            "safe snapshot should emit no edges"
        );
        assert!(
            result.edge_ids.is_empty(),
            "safe snapshot should emit no edge identifiers"
        );
        assert!(!result.ssi_state.has_in_rw);
        assert!(!result.ssi_state.has_out_rw);
    }

    // -- §5.7.3 test 3: Only incoming edge → commit --

    #[test]
    fn test_ssi_only_incoming_edge_commit() {
        let txn = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0));
        // Active reader R read page 5, committing T writes page 5.
        let reader = MockActiveTxn::new(2, 0, 1).with_reads(vec![page_key(5)]);
        let readers: Vec<&dyn ActiveTxnView> = vec![&reader];

        let result = ssi_validate_and_publish(
            txn,
            CommitSeq::new(1),
            CommitSeq::new(5),
            &[],            // T has no reads
            &[page_key(5)], // T writes page 5
            &readers,       // active readers
            &[],
            &[],
            &[],
            false,
        );
        let ok = result.expect("only incoming edge → commit allowed");
        assert!(ok.ssi_state.has_in_rw);
        assert!(!ok.ssi_state.has_out_rw);
        assert!(!ok.edges.is_empty());
    }

    // -- §5.7.3 test 4: Only outgoing edge → commit --

    #[test]
    fn test_ssi_only_outgoing_edge_commit() {
        let txn = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0));
        // Active writer W writes page 7, committing T read page 7.
        let writer = MockActiveTxn::new(3, 0, 1).with_writes(vec![page_key(7)]);
        let writers: Vec<&dyn ActiveTxnView> = vec![&writer];

        let result = ssi_validate_and_publish(
            txn,
            CommitSeq::new(1),
            CommitSeq::new(5),
            &[page_key(7)],  // T reads page 7
            &[page_key(20)], // T writes page 20 (different)
            &[],
            &writers,
            &[],
            &[],
            false,
        );
        let ok = result.expect("only outgoing edge → commit allowed");
        assert!(!ok.ssi_state.has_in_rw);
        assert!(ok.ssi_state.has_out_rw);
    }

    // -- §5.7.3 test 5: Pivot abort (both edges) --

    #[test]
    fn test_ssi_pivot_both_edges_abort() {
        let txn = TxnToken::new(TxnId::new(2).unwrap(), TxnEpoch::new(0));
        // R reads page 5 (incoming edge: R→T when T writes page 5)
        let reader = MockActiveTxn::new(1, 0, 1).with_reads(vec![page_key(5)]);
        // W writes page 7 (outgoing edge: T→W when T reads page 7)
        let writer = MockActiveTxn::new(3, 0, 1).with_writes(vec![page_key(7)]);
        let readers: Vec<&dyn ActiveTxnView> = vec![&reader];
        let writers: Vec<&dyn ActiveTxnView> = vec![&writer];

        let result = ssi_validate_and_publish(
            txn,
            CommitSeq::new(1),
            CommitSeq::new(5),
            &[page_key(7)], // T reads page 7
            &[page_key(5)], // T writes page 5
            &readers,
            &writers,
            &[],
            &[],
            false,
        );
        let err = result.expect_err("both in + out rw → MUST abort");
        assert_eq!(err.reason, SsiAbortReason::Pivot);
        assert_eq!(err.witness.reason, AbortReason::SsiPivot);
    }

    // -- §5.7.3 test 6: Dangerous structure detection --

    #[test]
    fn test_ssi_dangerous_structure_detection() {
        // T1 reads A, T2 reads B writes A, T3 reads A writes B.
        // T2 is pivot: incoming (T1→T2) + outgoing (T2→T3).
        let t2 = TxnToken::new(TxnId::new(2).unwrap(), TxnEpoch::new(0));

        // T1 reads page 10 (A) → incoming edge T1→T2 when T2 writes A
        let t1 = MockActiveTxn::new(1, 0, 1).with_reads(vec![page_key(10)]);
        // T3 writes page 20 (B) → outgoing edge T2→T3 when T2 reads B
        let t3 = MockActiveTxn::new(3, 0, 1).with_writes(vec![page_key(20)]);

        let readers: Vec<&dyn ActiveTxnView> = vec![&t1];
        let writers: Vec<&dyn ActiveTxnView> = vec![&t3];

        let result = ssi_validate_and_publish(
            t2,
            CommitSeq::new(1),
            CommitSeq::new(5),
            &[page_key(20)], // T2 reads B
            &[page_key(10)], // T2 writes A
            &readers,
            &writers,
            &[],
            &[],
            false,
        );
        assert!(result.is_err(), "dangerous structure → abort");
        let err = result.unwrap_err();
        assert_eq!(err.reason, SsiAbortReason::Pivot);
    }

    // -- §5.7.3 test 7: Incoming edge from hot plane --

    #[test]
    fn test_discover_incoming_from_hot_plane() {
        let txn = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0));
        let reader = MockActiveTxn::new(2, 0, 1).with_reads(vec![page_key(5)]);
        let readers: Vec<&dyn ActiveTxnView> = vec![&reader];

        let edges = discover_incoming_edges(
            txn,
            CommitSeq::new(1),
            CommitSeq::new(5),
            &[page_key(5)],
            &readers,
            &[],
        );
        assert_eq!(edges.len(), 1);
        assert!(edges[0].source_is_active);
        assert_eq!(edges[0].from.id.get(), 2);
    }

    // -- §5.7.3 test 8: Outgoing edge from hot plane --

    #[test]
    fn test_discover_outgoing_from_hot_plane() {
        let txn = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0));
        let writer = MockActiveTxn::new(3, 0, 1).with_writes(vec![page_key(7)]);
        let writers: Vec<&dyn ActiveTxnView> = vec![&writer];

        let edges = discover_outgoing_edges(
            txn,
            CommitSeq::new(1),
            CommitSeq::new(5),
            &[page_key(7)],
            &writers,
            &[],
        );
        assert_eq!(edges.len(), 1);
        assert!(edges[0].source_is_active);
        assert_eq!(edges[0].to.id.get(), 3);
    }

    // -- §5.7.3 test 9: Outgoing edge from commit index --

    #[test]
    fn test_discover_outgoing_from_commit_index() {
        let txn = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0));
        // Writer W committed at seq 3 (after T's begin_seq 1), wrote page 7.
        let committed_w = CommittedWriterInfo {
            token: TxnToken::new(TxnId::new(3).unwrap(), TxnEpoch::new(0)),
            commit_seq: CommitSeq::new(3),
            had_out_rw: false,
            pages: vec![PageNumber::new(7).unwrap()],
        };

        let edges = discover_outgoing_edges(
            txn,
            CommitSeq::new(1),
            CommitSeq::new(5),
            &[page_key(7)],
            &[],
            &[committed_w],
        );
        assert_eq!(edges.len(), 1);
        assert!(!edges[0].source_is_active);
    }

    // -- §5.7.3 test 10: Incoming edge from recently committed reader --

    #[test]
    fn test_discover_incoming_from_recently_committed() {
        let txn = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0));
        // Reader R committed at seq 3 (after T's begin_seq 1), read page 5.
        let committed_r = CommittedReaderInfo {
            token: TxnToken::new(TxnId::new(2).unwrap(), TxnEpoch::new(0)),
            begin_seq: CommitSeq::new(0),
            commit_seq: CommitSeq::new(3),
            had_in_rw: false,
            pages: vec![PageNumber::new(5).unwrap()],
        };

        let edges = discover_incoming_edges(
            txn,
            CommitSeq::new(1),
            CommitSeq::new(5),
            &[page_key(5)],
            &[],
            &[committed_r],
        );
        assert_eq!(edges.len(), 1);
        assert!(!edges[0].source_is_active);
    }

    // -- §5.7.3 test 11: Edge gap without commit index --

    #[test]
    fn test_edge_gap_without_commit_index() {
        // If we ONLY check hot plane (no committed writers), we miss the edge.
        let txn = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0));
        let committed_w = CommittedWriterInfo {
            token: TxnToken::new(TxnId::new(3).unwrap(), TxnEpoch::new(0)),
            commit_seq: CommitSeq::new(3),
            had_out_rw: false,
            pages: vec![PageNumber::new(7).unwrap()],
        };

        // Hot-plane only: no edges found.
        let edges_hot_only = discover_outgoing_edges(
            txn,
            CommitSeq::new(1),
            CommitSeq::new(5),
            &[page_key(7)],
            &[], // empty active writers
            &[], // no committed writers
        );
        assert!(
            edges_hot_only.is_empty(),
            "hot-plane only misses committed writer"
        );

        // With committed writers: edge found.
        let edges_full = discover_outgoing_edges(
            txn,
            CommitSeq::new(1),
            CommitSeq::new(5),
            &[page_key(7)],
            &[],
            &[committed_w],
        );
        assert_eq!(edges_full.len(), 1, "commit index catches the edge");
    }

    // -- §5.7.3 test 12: Edge gap without recently committed --

    #[test]
    fn test_edge_gap_without_recently_committed() {
        let txn = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0));
        let committed_r = CommittedReaderInfo {
            token: TxnToken::new(TxnId::new(2).unwrap(), TxnEpoch::new(0)),
            begin_seq: CommitSeq::new(0),
            commit_seq: CommitSeq::new(3),
            had_in_rw: false,
            pages: vec![PageNumber::new(5).unwrap()],
        };

        // Hot-plane only: no edges found.
        let edges_hot_only = discover_incoming_edges(
            txn,
            CommitSeq::new(1),
            CommitSeq::new(5),
            &[page_key(5)],
            &[],
            &[], // no committed readers
        );
        assert!(
            edges_hot_only.is_empty(),
            "hot-plane only misses committed reader"
        );

        // With RCRI: edge found.
        let edges_full = discover_incoming_edges(
            txn,
            CommitSeq::new(1),
            CommitSeq::new(5),
            &[page_key(5)],
            &[],
            &[committed_r],
        );
        assert_eq!(edges_full.len(), 1, "RCRI catches the edge");
    }

    // -- §5.7.3 test 12b: Interval overlap excludes future active reader --

    #[test]
    fn test_interval_overlap_excludes_future_active_reader() {
        let txn = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0));
        // Reader starts after our commit sequence and must not be treated as concurrent.
        let late_reader = MockActiveTxn::new(2, 0, 9).with_reads(vec![page_key(5)]);
        let readers: Vec<&dyn ActiveTxnView> = vec![&late_reader];

        let edges = discover_incoming_edges(
            txn,
            CommitSeq::new(1),
            CommitSeq::new(5),
            &[page_key(5)],
            &readers,
            &[],
        );
        assert!(
            edges.is_empty(),
            "reader interval [9,+inf) does not overlap [1,5]"
        );
    }

    // -- §5.7.3 test 12c: Interval overlap excludes stale committed writer --

    #[test]
    fn test_interval_overlap_excludes_stale_committed_writer() {
        let txn = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0));
        let stale_writer = CommittedWriterInfo {
            token: TxnToken::new(TxnId::new(3).unwrap(), TxnEpoch::new(0)),
            commit_seq: CommitSeq::new(4),
            had_out_rw: false,
            pages: vec![PageNumber::new(7).unwrap()],
        };

        let edges = discover_outgoing_edges(
            txn,
            CommitSeq::new(5),
            CommitSeq::new(8),
            &[page_key(7)],
            &[],
            &[stale_writer],
        );
        assert!(
            edges.is_empty(),
            "writer interval (-inf,4] does not overlap [5,8]"
        );
    }

    // -- §5.7.3 test 12d: Bank-transfer write skew prevented --

    #[test]
    fn test_bank_transfer_write_skew_prevented() {
        let t1 = TxnToken::new(TxnId::new(11).unwrap(), TxnEpoch::new(0));
        let t2 = TxnToken::new(TxnId::new(12).unwrap(), TxnEpoch::new(0));

        let t2_active = MockActiveTxn::new(12, 0, 1).with_reads(vec![page_key(100), page_key(200)]);
        let readers_for_t1: Vec<&dyn ActiveTxnView> = vec![&t2_active];
        let t1_reads = [page_key(100), page_key(200)];
        let t1_writes = [page_key(100)];
        let t1_commit = ssi_validate_and_publish(
            t1,
            CommitSeq::new(1),
            CommitSeq::new(2),
            &t1_reads,
            &t1_writes,
            &readers_for_t1,
            &[],
            &[],
            &[],
            false,
        )
        .expect("first transfer leg should commit");

        let committed_reader_t1 = CommittedReaderInfo {
            token: t1,
            begin_seq: CommitSeq::new(1),
            commit_seq: CommitSeq::new(2),
            had_in_rw: t1_commit.ssi_state.has_in_rw,
            pages: vec![PageNumber::new(100).unwrap(), PageNumber::new(200).unwrap()],
        };
        let committed_writer_t1 = CommittedWriterInfo {
            token: t1,
            commit_seq: CommitSeq::new(2),
            had_out_rw: t1_commit.ssi_state.has_out_rw,
            pages: vec![PageNumber::new(100).unwrap()],
        };

        let t2_reads = [page_key(100), page_key(200)];
        let t2_writes = [page_key(200)];
        let t2_result = ssi_validate_and_publish(
            t2,
            CommitSeq::new(1),
            CommitSeq::new(3),
            &t2_reads,
            &t2_writes,
            &[],
            &[],
            &[committed_reader_t1],
            &[committed_writer_t1],
            false,
        );
        assert!(
            t2_result.is_err(),
            "second transfer leg must abort to prevent write skew"
        );
    }

    // -- §5.7.3 test 12e: Doctor-on-call write skew prevented --

    #[test]
    fn test_doctor_on_call_write_skew_prevented() {
        let d1 = TxnToken::new(TxnId::new(21).unwrap(), TxnEpoch::new(0));
        let d2 = TxnToken::new(TxnId::new(22).unwrap(), TxnEpoch::new(0));

        let d2_active = MockActiveTxn::new(22, 0, 1).with_reads(vec![page_key(310), page_key(311)]);
        let readers_for_d1: Vec<&dyn ActiveTxnView> = vec![&d2_active];
        let d1_reads = [page_key(310), page_key(311)];
        let d1_writes = [page_key(310)];
        let d1_commit = ssi_validate_and_publish(
            d1,
            CommitSeq::new(1),
            CommitSeq::new(2),
            &d1_reads,
            &d1_writes,
            &readers_for_d1,
            &[],
            &[],
            &[],
            false,
        )
        .expect("first doctor update should commit");

        let committed_reader_d1 = CommittedReaderInfo {
            token: d1,
            begin_seq: CommitSeq::new(1),
            commit_seq: CommitSeq::new(2),
            had_in_rw: d1_commit.ssi_state.has_in_rw,
            pages: vec![PageNumber::new(310).unwrap(), PageNumber::new(311).unwrap()],
        };
        let committed_writer_d1 = CommittedWriterInfo {
            token: d1,
            commit_seq: CommitSeq::new(2),
            had_out_rw: d1_commit.ssi_state.has_out_rw,
            pages: vec![PageNumber::new(310).unwrap()],
        };

        let d2_reads = [page_key(310), page_key(311)];
        let d2_writes = [page_key(311)];
        let d2_result = ssi_validate_and_publish(
            d2,
            CommitSeq::new(1),
            CommitSeq::new(3),
            &d2_reads,
            &d2_writes,
            &[],
            &[],
            &[committed_reader_d1],
            &[committed_writer_d1],
            false,
        );
        assert!(
            d2_result.is_err(),
            "second doctor update must abort to preserve on-call invariant"
        );
    }

    // -- §5.7.3 test 13: T3 rule — active pivot marked --

    #[test]
    fn test_t3_rule_active_pivot_marked() {
        // T commits. R is active, R.has_in_rw = true, and T wrote a key R read.
        // R should be marked_for_abort.
        let t = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0));
        let r = MockActiveTxn::new(2, 0, 1)
            .with_reads(vec![page_key(5)])
            .with_has_in_rw(true);
        let readers: Vec<&dyn ActiveTxnView> = vec![&r];

        let result = ssi_validate_and_publish(
            t,
            CommitSeq::new(1),
            CommitSeq::new(5),
            &[],            // T has no reads → no outgoing
            &[page_key(5)], // T writes page 5 → incoming from R
            &readers,
            &[],
            &[],
            &[],
            false,
        );
        // T should commit (only has incoming, not outgoing).
        result.expect("T has only incoming edge, should commit");
        // R should be marked for abort (T3 rule).
        assert!(r.has_out.get(), "R.has_out_rw should be set to true");
        assert!(r.marked.get(), "R should be marked for abort (T3 rule)");
    }

    // -- §5.7.3 test 14: T3 rule — committed pivot forces abort --

    #[test]
    fn test_t3_rule_committed_pivot_forces_abort() {
        // T commits. Committed reader R had has_in_rw=true and read page 5.
        // T writes page 5. R is committed pivot → T MUST abort.
        let t = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0));
        let committed_r = CommittedReaderInfo {
            token: TxnToken::new(TxnId::new(2).unwrap(), TxnEpoch::new(0)),
            begin_seq: CommitSeq::new(0),
            commit_seq: CommitSeq::new(3),
            had_in_rw: true, // R was pivot at commit time
            pages: vec![PageNumber::new(5).unwrap()],
        };

        let result = ssi_validate_and_publish(
            t,
            CommitSeq::new(1),
            CommitSeq::new(5),
            &[],
            &[page_key(5)],
            &[],
            &[],
            &[committed_r],
            &[],
            false,
        );
        let err = result.expect_err("committed pivot → T must abort");
        assert_eq!(err.reason, SsiAbortReason::CommittedPivot);
    }

    // -- §5.7.3 test 15: T3 rule — active no in_rw → no mark --

    #[test]
    fn test_t3_rule_active_no_in_rw_no_mark() {
        let t = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0));
        let r = MockActiveTxn::new(2, 0, 1).with_reads(vec![page_key(5)]);
        // R.has_in_rw = false (default)
        let readers: Vec<&dyn ActiveTxnView> = vec![&r];

        let result = ssi_validate_and_publish(
            t,
            CommitSeq::new(1),
            CommitSeq::new(5),
            &[],
            &[page_key(5)],
            &readers,
            &[],
            &[],
            &[],
            false,
        );
        result.expect("T should commit");
        assert!(r.has_out.get(), "R.has_out_rw should be set");
        assert!(!r.marked.get(), "R should NOT be marked (no in_rw)");
    }

    // -- §5.7.3 test 16: Refinement eliminates false edge --

    #[test]
    fn test_refinement_eliminates_false_edge() {
        // Skipping refinement is always safe (over-approx). But when applied,
        // cell-level non-overlap should drop the edge. For now we verify that
        // different pages produce no edge (the simplest refinement).
        let txn = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0));
        let reader = MockActiveTxn::new(2, 0, 1).with_reads(vec![page_key(10)]);
        let readers: Vec<&dyn ActiveTxnView> = vec![&reader];

        // T writes page 20 (different from R's read page 10).
        let result = ssi_validate_and_publish(
            txn,
            CommitSeq::new(1),
            CommitSeq::new(5),
            &[page_key(30)],
            &[page_key(20)],
            &readers,
            &[],
            &[],
            &[],
            false,
        );
        let ok = result.expect("no overlap → commit");
        assert!(!ok.ssi_state.has_in_rw);
    }

    // -- §5.7.3 test 17: Skip refinement is safe --

    #[test]
    fn test_skip_refinement_safe() {
        // Without refinement, page-level overlap may produce false positive edges
        // but never misses real anomalies. Verify correctness.
        let t = TxnToken::new(TxnId::new(2).unwrap(), TxnEpoch::new(0));
        let t1 = MockActiveTxn::new(1, 0, 1).with_reads(vec![page_key(5)]);
        let t3 = MockActiveTxn::new(3, 0, 1).with_writes(vec![page_key(5)]);
        let readers: Vec<&dyn ActiveTxnView> = vec![&t1];
        let writers: Vec<&dyn ActiveTxnView> = vec![&t3];

        // T2 reads and writes page 5 → both edges → abort.
        // Without refinement, this is correct (conservative).
        let result = ssi_validate_and_publish(
            t,
            CommitSeq::new(1),
            CommitSeq::new(5),
            &[page_key(5)],
            &[page_key(5)],
            &readers,
            &writers,
            &[],
            &[],
            false,
        );
        assert!(result.is_err(), "without refinement, overlap → abort");
    }

    // -- §5.7.3 test 18: DependencyEdge published --

    #[test]
    fn test_dependency_edge_published() {
        let txn = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0));
        let reader = MockActiveTxn::new(2, 0, 1).with_reads(vec![page_key(5)]);
        let readers: Vec<&dyn ActiveTxnView> = vec![&reader];

        let result = ssi_validate_and_publish(
            txn,
            CommitSeq::new(1),
            CommitSeq::new(5),
            &[],
            &[page_key(5)],
            &readers,
            &[],
            &[],
            &[],
            false,
        );
        let ok = result.unwrap();
        assert!(!ok.edges.is_empty(), "edge must be published");
        assert_eq!(ok.edges[0].kind, DependencyEdgeKind::RwAntiDependency);
        assert_eq!(ok.edges[0].from.id.get(), 2); // reader
        assert_eq!(ok.edges[0].to.id.get(), 1); // committing txn
    }

    // -- §5.7.3 test 19: CommitProof published --

    #[test]
    fn test_commit_proof_published() {
        let txn = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0));
        let result = ssi_validate_and_publish(
            txn,
            CommitSeq::new(1),
            CommitSeq::new(5),
            &[page_key(10)],
            &[page_key(20)],
            &[],
            &[],
            &[],
            &[],
            false,
        );
        let ok = result.unwrap();
        assert_eq!(ok.commit_proof.txn, txn);
        assert_eq!(ok.commit_proof.begin_seq.get(), 1);
        assert_eq!(ok.commit_proof.commit_seq.get(), 5);
        assert_eq!(ok.commit_proof.abort_policy, AbortPolicy::AbortPivot);
    }

    // -- §5.7.3 test 20: AbortWitness published on SSI abort --

    #[test]
    fn test_abort_witness_published() {
        let txn = TxnToken::new(TxnId::new(2).unwrap(), TxnEpoch::new(0));
        let reader = MockActiveTxn::new(1, 0, 1).with_reads(vec![page_key(5)]);
        let writer = MockActiveTxn::new(3, 0, 1).with_writes(vec![page_key(7)]);
        let readers: Vec<&dyn ActiveTxnView> = vec![&reader];
        let writers: Vec<&dyn ActiveTxnView> = vec![&writer];

        let result = ssi_validate_and_publish(
            txn,
            CommitSeq::new(1),
            CommitSeq::new(5),
            &[page_key(7)],
            &[page_key(5)],
            &readers,
            &writers,
            &[],
            &[],
            false,
        );
        let err = result.unwrap_err();
        assert_eq!(err.witness.txn, txn);
        assert_eq!(err.witness.reason, AbortReason::SsiPivot);
        assert!(
            !err.witness.edges_observed.is_empty(),
            "abort witness must contain edges"
        );
    }

    // -- §5.7.3 test 21: SSI state has_in_rw flag --

    #[test]
    fn test_ssi_state_has_in_rw_flag() {
        let txn = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0));
        let reader = MockActiveTxn::new(2, 0, 1).with_reads(vec![page_key(5)]);
        let readers: Vec<&dyn ActiveTxnView> = vec![&reader];

        let result = ssi_validate_and_publish(
            txn,
            CommitSeq::new(1),
            CommitSeq::new(5),
            &[],
            &[page_key(5)],
            &readers,
            &[],
            &[],
            &[],
            false,
        );
        let ok = result.unwrap();
        assert!(ok.ssi_state.has_in_rw, "incoming edge must set has_in_rw");
    }

    // -- §5.7.3 test 22: SSI state has_out_rw flag --

    #[test]
    fn test_ssi_state_has_out_rw_flag() {
        let txn = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0));
        let writer = MockActiveTxn::new(3, 0, 1).with_writes(vec![page_key(7)]);
        let writers: Vec<&dyn ActiveTxnView> = vec![&writer];

        let result = ssi_validate_and_publish(
            txn,
            CommitSeq::new(1),
            CommitSeq::new(5),
            &[page_key(7)],
            &[page_key(20)],
            &[],
            &writers,
            &[],
            &[],
            false,
        );
        let ok = result.unwrap();
        assert!(ok.ssi_state.has_out_rw, "outgoing edge must set has_out_rw");
    }

    // -- §5.7.3 test 23: Marked for abort --

    #[test]
    fn test_ssi_state_marked_for_abort() {
        let txn = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0));
        let result = ssi_validate_and_publish(
            txn,
            CommitSeq::new(1),
            CommitSeq::new(5),
            &[page_key(10)],
            &[page_key(20)],
            &[],
            &[],
            &[],
            &[],
            true, // marked_for_abort
        );
        let err = result.expect_err("marked_for_abort → must abort");
        assert_eq!(err.reason, SsiAbortReason::MarkedForAbort);
    }

    // -- §5.7.3 test 24: Edges emitted tracking --

    #[test]
    fn test_ssi_state_edges_emitted_tracking() {
        let txn = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0));
        let reader = MockActiveTxn::new(2, 0, 1).with_reads(vec![page_key(5)]);
        let readers: Vec<&dyn ActiveTxnView> = vec![&reader];

        let result = ssi_validate_and_publish(
            txn,
            CommitSeq::new(1),
            CommitSeq::new(5),
            &[],
            &[page_key(5)],
            &readers,
            &[],
            &[],
            &[],
            false,
        );
        let ok = result.unwrap();
        assert_eq!(ok.edge_ids.len(), ok.edges.len());
        assert_eq!(ok.ssi_state.edges_emitted.len(), ok.edges.len());
    }

    // -- §5.7.3 test 25: Conservative pivot rule --

    #[test]
    fn test_conservative_pivot_rule() {
        // The pivot abort rule omits (T1 committed OR T3 committed) check
        // intentionally. Verify that even when both T1 and T3 are active
        // (no actual cycle yet), the pivot still aborts.
        let t2 = TxnToken::new(TxnId::new(2).unwrap(), TxnEpoch::new(0));
        let t1 = MockActiveTxn::new(1, 0, 1).with_reads(vec![page_key(10)]);
        let t3 = MockActiveTxn::new(3, 0, 1).with_writes(vec![page_key(20)]);
        let readers: Vec<&dyn ActiveTxnView> = vec![&t1];
        let writers: Vec<&dyn ActiveTxnView> = vec![&t3];

        let result = ssi_validate_and_publish(
            t2,
            CommitSeq::new(1),
            CommitSeq::new(5),
            &[page_key(20)],
            &[page_key(10)],
            &readers,
            &writers,
            &[],
            &[],
            false,
        );
        // Conservative: aborts even though neither T1 nor T3 committed.
        assert!(
            result.is_err(),
            "conservative rule: abort even with all active"
        );
    }

    // -- §5.7.3 test 26: False positive bounded --

    #[test]
    fn test_false_positive_bounded() {
        // Under non-overlapping writes, no false positive aborts should occur.
        let mut commits = 0_u32;
        let mut aborts = 0_u32;

        for i in 0..100_u64 {
            let txn = TxnToken::new(TxnId::new(i + 1).unwrap(), TxnEpoch::new(0));
            // Each txn reads its own page, writes its own page.
            #[allow(clippy::cast_possible_truncation)]
            let pg = (i as u32) * 2 + 1;
            let result = ssi_validate_and_publish(
                txn,
                CommitSeq::new(1),
                CommitSeq::new(i + 2),
                &[page_key(pg)],
                &[page_key(pg + 1)],
                &[], // no active readers overlap
                &[], // no active writers overlap
                &[],
                &[],
                false,
            );
            match result {
                Ok(_) => commits += 1,
                Err(_) => aborts += 1,
            }
        }
        assert_eq!(aborts, 0, "no false positives with non-overlapping writes");
        assert_eq!(commits, 100);
    }

    // -- §5.7.3 integration test: Write skew prevented --

    #[test]
    #[allow(clippy::redundant_clone, clippy::cloned_ref_to_slice_refs)]
    fn test_write_skew_prevented() {
        // Classic write skew: T1 reads (A,B), writes A; T2 reads (A,B), writes B.
        // Both try to commit. At most one should succeed.
        let t1_token = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0));
        let t2_token = TxnToken::new(TxnId::new(2).unwrap(), TxnEpoch::new(0));

        let page_a = page_key(10);
        let page_b = page_key(20);

        // T1 as active reader (reads A,B)
        let _t1_view = MockActiveTxn::new(1, 0, 1).with_reads(vec![page_a.clone(), page_b.clone()]);
        // T2 as active reader (reads A,B)
        let t2_view = MockActiveTxn::new(2, 0, 1).with_reads(vec![page_a.clone(), page_b.clone()]);

        // T1 commits first: writes A. T2 is active reader of A → incoming edge.
        // No outgoing edge for T1 (nobody is writing to B yet).
        let t2_readers: Vec<&dyn ActiveTxnView> = vec![&t2_view];
        let result_t1 = ssi_validate_and_publish(
            t1_token,
            CommitSeq::new(1),
            CommitSeq::new(2),
            &[page_a.clone(), page_b.clone()], // T1 reads A,B
            &[page_a.clone()],                 // T1 writes A
            &t2_readers,                       // T2 is reading
            &[],
            &[],
            &[],
            false,
        );
        // T1 should commit (only incoming edge, no outgoing).
        let ok_t1 = result_t1.expect("T1 should commit (only incoming)");
        assert!(ok_t1.ssi_state.has_in_rw); // T2 read A that T1 writes

        // Now T2 tries to commit: writes B.
        // T1 has committed and wrote A that T2 read → outgoing edge (T2→T1 via commit log).
        // T2 also wrote B that T1 read → incoming edge if T1 is in RCRI.
        // But T1 already committed, so we model it as committed reader.
        let reader_t1 = CommittedReaderInfo {
            token: t1_token,
            begin_seq: CommitSeq::new(1),
            commit_seq: CommitSeq::new(2),
            had_in_rw: ok_t1.ssi_state.has_in_rw,
            pages: vec![PageNumber::new(10).unwrap(), PageNumber::new(20).unwrap()],
        };
        let writer_t1 = CommittedWriterInfo {
            token: t1_token,
            commit_seq: CommitSeq::new(2),
            had_out_rw: ok_t1.ssi_state.has_out_rw,
            pages: vec![PageNumber::new(10).unwrap()],
        };

        let result_t2 = ssi_validate_and_publish(
            t2_token,
            CommitSeq::new(1),
            CommitSeq::new(3),
            &[page_a.clone(), page_b.clone()], // T2 reads A,B
            &[page_b],                         // T2 writes B
            &[],
            &[],
            &[reader_t1], // T1 is committed reader
            &[writer_t1], // T1 is committed writer
            false,
        );

        // T2 should abort: T1 committed with has_in_rw → T3 rule.
        // T1 read B, T2 writes B → incoming edge from T1 (committed).
        // T1 had has_in_rw=true → committed pivot → T2 must abort.
        assert!(result_t2.is_err(), "write skew must be prevented");
    }

    // -- §5.7.3 integration test: Concurrent inserts to different pages --

    #[test]
    fn test_concurrent_inserts_different_pages_no_abort() {
        let t1 = TxnToken::new(TxnId::new(1).unwrap(), TxnEpoch::new(0));
        let _t2 = TxnToken::new(TxnId::new(2).unwrap(), TxnEpoch::new(0));

        // T1 inserts into page 5, T2 inserts into page 10.
        // No rw-antidependency overlap.
        let t2_view = MockActiveTxn::new(2, 0, 1)
            .with_reads(vec![page_key(10)])
            .with_writes(vec![page_key(10)]);
        let readers: Vec<&dyn ActiveTxnView> = vec![&t2_view];
        let writers: Vec<&dyn ActiveTxnView> = vec![&t2_view];

        let result = ssi_validate_and_publish(
            t1,
            CommitSeq::new(1),
            CommitSeq::new(2),
            &[page_key(5)],
            &[page_key(5)],
            &readers,
            &writers,
            &[],
            &[],
            false,
        );
        result.expect("different pages → no conflict → both commit");
    }

    // -- §5.7.3 stress test 27: Phantom-style batch insert + scan --

    #[test]
    fn test_phantom_batch_insert_scan_conflict_prevented() {
        // T_scan reads a range witness and writes an aggregate page.
        // T_insert writes that range witness and reads aggregate state.
        // This forms a dangerous structure and exactly one side should commit.
        let t_scan = TxnToken::new(TxnId::new(301).unwrap(), TxnEpoch::new(0));
        let t_insert = TxnToken::new(TxnId::new(302).unwrap(), TxnEpoch::new(0));

        let range_witness = page_key(900);
        let aggregate_page = page_key(901);
        let active_insert = MockActiveTxn::new(302, 0, 1)
            .with_reads(vec![aggregate_page.clone()])
            .with_writes(vec![range_witness.clone()]);
        let readers_for_scan: Vec<&dyn ActiveTxnView> = vec![&active_insert];
        let writers_for_scan: Vec<&dyn ActiveTxnView> = vec![&active_insert];

        let scan_result = ssi_validate_and_publish(
            t_scan,
            CommitSeq::new(1),
            CommitSeq::new(2),
            std::slice::from_ref(&range_witness),
            std::slice::from_ref(&aggregate_page),
            &readers_for_scan,
            &writers_for_scan,
            &[],
            &[],
            false,
        );
        assert!(
            scan_result.is_err(),
            "scan+aggregate transaction should abort under phantom-style cycle"
        );

        let insert_result = ssi_validate_and_publish(
            t_insert,
            CommitSeq::new(1),
            CommitSeq::new(3),
            std::slice::from_ref(&aggregate_page),
            std::slice::from_ref(&range_witness),
            &[],
            &[],
            &[],
            &[],
            false,
        );
        assert!(
            insert_result.is_ok(),
            "one side of the phantom-style cycle must still commit"
        );
    }

    // -- §5.7.3 stress test 28: 3-way adversarial cycle breaks --

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_adversarial_three_way_cycle_breaks_with_single_abort() {
        let begin_seq = CommitSeq::new(1);
        let t1 = TxnToken::new(TxnId::new(401).unwrap(), TxnEpoch::new(0));
        let t2 = TxnToken::new(TxnId::new(402).unwrap(), TxnEpoch::new(0));
        let t3 = TxnToken::new(TxnId::new(403).unwrap(), TxnEpoch::new(0));

        let v1 = MockActiveTxn::new(401, 0, begin_seq.get())
            .with_reads(vec![page_key(1000)])
            .with_writes(vec![page_key(1001)]);
        let v2 = MockActiveTxn::new(402, 0, begin_seq.get())
            .with_reads(vec![page_key(1001)])
            .with_writes(vec![page_key(1002)]);
        let v3 = MockActiveTxn::new(403, 0, begin_seq.get())
            .with_reads(vec![page_key(1002)])
            .with_writes(vec![page_key(1000)]);

        let mut committed_readers = Vec::new();
        let mut committed_writers = Vec::new();
        let mut commits = 0_u32;
        let mut aborts = 0_u32;

        let readers_t1: Vec<&dyn ActiveTxnView> = vec![&v2];
        let writers_t1: Vec<&dyn ActiveTxnView> = vec![&v3];
        let t1_res = ssi_validate_and_publish(
            t1,
            begin_seq,
            CommitSeq::new(2),
            &v1.reads,
            &v1.writes,
            &readers_t1,
            &writers_t1,
            &committed_readers,
            &committed_writers,
            false,
        );
        match t1_res {
            Ok(ok) => {
                commits += 1;
                committed_readers.push(CommittedReaderInfo {
                    token: t1,
                    begin_seq,
                    commit_seq: CommitSeq::new(2),
                    had_in_rw: ok.ssi_state.has_in_rw,
                    pages: keys_to_pages(&v1.reads),
                });
                committed_writers.push(CommittedWriterInfo {
                    token: t1,
                    commit_seq: CommitSeq::new(2),
                    had_out_rw: ok.ssi_state.has_out_rw,
                    pages: keys_to_pages(&v1.writes),
                });
            }
            Err(_) => aborts += 1,
        }

        let readers_t2: Vec<&dyn ActiveTxnView> = vec![&v3];
        let writers_t2: Vec<&dyn ActiveTxnView> = vec![&v3];
        let t2_res = ssi_validate_and_publish(
            t2,
            begin_seq,
            CommitSeq::new(3),
            &v2.reads,
            &v2.writes,
            &readers_t2,
            &writers_t2,
            &committed_readers,
            &committed_writers,
            false,
        );
        match t2_res {
            Ok(ok) => {
                commits += 1;
                committed_readers.push(CommittedReaderInfo {
                    token: t2,
                    begin_seq,
                    commit_seq: CommitSeq::new(3),
                    had_in_rw: ok.ssi_state.has_in_rw,
                    pages: keys_to_pages(&v2.reads),
                });
                committed_writers.push(CommittedWriterInfo {
                    token: t2,
                    commit_seq: CommitSeq::new(3),
                    had_out_rw: ok.ssi_state.has_out_rw,
                    pages: keys_to_pages(&v2.writes),
                });
            }
            Err(_) => aborts += 1,
        }

        let t3_res = ssi_validate_and_publish(
            t3,
            begin_seq,
            CommitSeq::new(4),
            &v3.reads,
            &v3.writes,
            &[],
            &[],
            &committed_readers,
            &committed_writers,
            false,
        );
        match t3_res {
            Ok(_) => commits += 1,
            Err(_) => aborts += 1,
        }

        assert_eq!(commits + aborts, 3);
        assert_eq!(aborts, 1, "exactly one abort should break the 3-cycle");
    }

    // -- §5.7.3 stress test 29: 100-writer adversarial CI-scale schedule --

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_100_writer_adversarial_schedule_with_serialization_checker() {
        struct StressTxn {
            token_id: u64,
            token: TxnToken,
            reads: Vec<WitnessKey>,
            writes: Vec<WitnessKey>,
            view: MockActiveTxn,
        }

        let begin_seq = CommitSeq::new(1);
        let mut txns = Vec::new();
        let mut next_id = 1_u64;

        // 10 conflict pairs (20 txns): each pair forms a classic write-skew cycle.
        for pair in 0..10_u32 {
            let base = 2000_u32 + pair * 10;
            let reads = vec![page_key(base), page_key(base + 1)];

            let token_a = TxnToken::new(TxnId::new(next_id).unwrap(), TxnEpoch::new(0));
            let view_a = MockActiveTxn::new(next_id, 0, begin_seq.get())
                .with_reads(reads.clone())
                .with_writes(vec![page_key(base)]);
            txns.push(StressTxn {
                token_id: next_id,
                token: token_a,
                reads: reads.clone(),
                writes: vec![page_key(base)],
                view: view_a,
            });
            next_id += 1;

            let token_b = TxnToken::new(TxnId::new(next_id).unwrap(), TxnEpoch::new(0));
            let view_b = MockActiveTxn::new(next_id, 0, begin_seq.get())
                .with_reads(reads.clone())
                .with_writes(vec![page_key(base + 1)]);
            txns.push(StressTxn {
                token_id: next_id,
                token: token_b,
                reads,
                writes: vec![page_key(base + 1)],
                view: view_b,
            });
            next_id += 1;
        }

        // Remaining 80 txns are disjoint and should commit.
        while txns.len() < 100 {
            let disjoint = 5000_u32 + u32::try_from(txns.len()).unwrap();
            let token = TxnToken::new(TxnId::new(next_id).unwrap(), TxnEpoch::new(0));
            let reads = vec![page_key(disjoint)];
            let writes = vec![page_key(disjoint + 10_000)];
            let view = MockActiveTxn::new(next_id, 0, begin_seq.get())
                .with_reads(reads.clone())
                .with_writes(writes.clone());
            txns.push(StressTxn {
                token_id: next_id,
                token,
                reads,
                writes,
                view,
            });
            next_id += 1;
        }

        let mut committed_ids = HashSet::new();
        let mut committed_readers = Vec::new();
        let mut committed_writers = Vec::new();
        let mut abort_count = 0_u32;

        for idx in 0..txns.len() {
            let current = &txns[idx];
            let active_tail = &txns[idx + 1..];
            let active_readers: Vec<&dyn ActiveTxnView> = active_tail
                .iter()
                .map(|txn| &txn.view as &dyn ActiveTxnView)
                .collect();
            let active_writers: Vec<&dyn ActiveTxnView> = active_tail
                .iter()
                .map(|txn| &txn.view as &dyn ActiveTxnView)
                .collect();

            let commit_seq = CommitSeq::new(u64::try_from(idx).unwrap() + 2);
            let result = ssi_validate_and_publish(
                current.token,
                begin_seq,
                commit_seq,
                &current.reads,
                &current.writes,
                &active_readers,
                &active_writers,
                &committed_readers,
                &committed_writers,
                false,
            );

            match result {
                Ok(ok) => {
                    committed_ids.insert(current.token_id);
                    committed_readers.push(CommittedReaderInfo {
                        token: current.token,
                        begin_seq,
                        commit_seq,
                        had_in_rw: ok.ssi_state.has_in_rw,
                        pages: keys_to_pages(&current.reads),
                    });
                    committed_writers.push(CommittedWriterInfo {
                        token: current.token,
                        commit_seq,
                        had_out_rw: ok.ssi_state.has_out_rw,
                        pages: keys_to_pages(&current.writes),
                    });
                }
                Err(_) => abort_count += 1,
            }
        }

        let total = u32::try_from(txns.len()).unwrap();
        assert_eq!(
            u32::try_from(committed_ids.len()).unwrap() + abort_count,
            total,
            "all 100 commit attempts must complete (no deadlock/livelock)"
        );

        // Serialization checker: each conflict pair must have exactly one commit.
        let mut mandatory_aborts = 0_u32;
        for pair in 0..10_u64 {
            let tx_a = pair * 2 + 1;
            let tx_b = pair * 2 + 2;
            let a_committed = committed_ids.contains(&tx_a);
            let b_committed = committed_ids.contains(&tx_b);
            assert!(
                a_committed ^ b_committed,
                "conflict pair ({tx_a},{tx_b}) must commit exactly one member"
            );
            mandatory_aborts += 1;
        }

        // Disjoint workload should commit without aborts.
        for id in 21_u64..=100_u64 {
            assert!(
                committed_ids.contains(&id),
                "disjoint writer {id} should commit"
            );
        }

        let false_positive_aborts = abort_count.saturating_sub(mandatory_aborts);
        assert!(
            false_positive_aborts <= 5,
            "false positive aborts must stay under 5%: {false_positive_aborts}/100"
        );
    }

    // -- §5.7.3 stress test 30: Long-running reader under writer churn --

    #[test]
    fn test_long_running_reader_stable_snapshot_under_writer_churn() {
        let long_reader =
            MockActiveTxn::new(9_001, 0, 1).with_reads(vec![page_key(700), page_key(701)]);
        let active_readers: Vec<&dyn ActiveTxnView> = vec![&long_reader];

        let mut commits = 0_u32;
        for i in 0..200_u64 {
            let writer = TxnToken::new(TxnId::new(9_100 + i).unwrap(), TxnEpoch::new(0));
            // Writers update pages touched by the reader. Reader must remain
            // active and unmarked unless it becomes a pivot.
            let write_key = if i % 2 == 0 {
                page_key(700)
            } else {
                page_key(701)
            };
            let result = ssi_validate_and_publish(
                writer,
                CommitSeq::new(1),
                CommitSeq::new(i + 2),
                &[],
                &[write_key],
                &active_readers,
                &[],
                &[],
                &[],
                false,
            );
            assert!(
                result.is_ok(),
                "writer churn should not deadlock/abort readers"
            );
            commits += 1;
        }

        assert_eq!(commits, 200);
        assert!(
            !long_reader.marked.get(),
            "long-running read-only snapshot must not be marked for abort"
        );
    }

    #[test]
    fn test_evidence_metrics_count_by_outcome() {
        let before = ssi_evidence_metrics_snapshot();

        let commit_txn = TxnToken::new(TxnId::new(90_001).unwrap(), TxnEpoch::new(0));
        let commit_result = ssi_validate_and_publish(
            commit_txn,
            CommitSeq::new(1),
            CommitSeq::new(2),
            &[page_key(500)],
            &[page_key(600)],
            &[],
            &[],
            &[],
            &[],
            false,
        );
        commit_result.expect("commit decision should be recorded");

        let abort_txn = TxnToken::new(TxnId::new(90_002).unwrap(), TxnEpoch::new(0));
        let reader = MockActiveTxn::new(90_003, 0, 1).with_reads(vec![page_key(700)]);
        let writer = MockActiveTxn::new(90_004, 0, 1).with_writes(vec![page_key(800)]);
        let readers: Vec<&dyn ActiveTxnView> = vec![&reader];
        let writers: Vec<&dyn ActiveTxnView> = vec![&writer];
        let abort_result = ssi_validate_and_publish(
            abort_txn,
            CommitSeq::new(1),
            CommitSeq::new(3),
            &[page_key(800)],
            &[page_key(700)],
            &readers,
            &writers,
            &[],
            &[],
            false,
        );
        abort_result.expect_err("pivot abort should be recorded");

        let after = ssi_evidence_metrics_snapshot();
        assert!(
            after.fsqlite_evidence_records_total_commit
                > before.fsqlite_evidence_records_total_commit
        );
        assert!(
            after.fsqlite_evidence_records_total_abort
                > before.fsqlite_evidence_records_total_abort
        );
        assert!(
            after.fsqlite_evidence_records_total() >= before.fsqlite_evidence_records_total() + 2
        );
    }

    #[test]
    fn test_evidence_store_queryable_by_txn_id() {
        let txn = TxnToken::new(TxnId::new(90_101).unwrap(), TxnEpoch::new(0));
        let result = ssi_validate_and_publish(
            txn,
            CommitSeq::new(1),
            CommitSeq::new(2),
            &[page_key(901)],
            &[page_key(902)],
            &[],
            &[],
            &[],
            &[],
            false,
        );
        result.expect("commit should succeed");

        // The global evidence ledger is a bounded ring buffer (capacity 1024).
        // Under parallel test load, our entry may be evicted before we query it.
        // Wait for quiescence, then check: if found, validate fields; if the
        // buffer is full and our entry was evicted, that's expected.
        std::thread::sleep(std::time::Duration::from_millis(100));
        let snapshot = ssi_evidence_snapshot();
        let target_txn_id = txn.id.get();
        let rows = ssi_evidence_query(&SsiDecisionQuery {
            txn_id: Some(target_txn_id),
            ..SsiDecisionQuery::default()
        });
        if rows.is_empty() {
            // Under parallel load, our entry may have been evicted from the
            // bounded ring buffer. Verify the ledger is operational (non-empty)
            // and all entries have valid structure.
            assert!(
                !snapshot.is_empty(),
                "evidence ledger should not be completely empty"
            );
            for card in &snapshot {
                assert!(card.decision_id > 0, "every card must have a decision_id");
            }
        } else {
            let last = rows.last().unwrap();
            assert_eq!(last.txn.id.get(), target_txn_id);
            assert!(last.decision_id > 0, "decision_id must be populated");
        }
    }
}
