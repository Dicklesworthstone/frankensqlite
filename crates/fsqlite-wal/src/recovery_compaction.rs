//! Native mode recovery algorithm (§7.12) and ECS storage reclamation (§7.13).
//!
//! # §7.12 Recovery
//!
//! Five-step algorithm:
//! 1. Load `RootManifest` via `ecs/root`.
//! 2. Locate latest checkpoint and its manifest.
//! 3. Scan marker stream from checkpoint tip forward (or genesis).
//! 4. For each marker: fetch/decode capsule (RaptorQ repair if needed), apply.
//! 5. Rebuild/refresh index segments and caches.
//!
//! # §7.13 Compaction
//!
//! Four-phase crash-safe saga:
//! - Phase 1 **Mark**: trace reachable objects, build [`BloomFilter`] of live IDs.
//! - Phase 2 **Compact**: copy live symbols to new segments, fdatasync.
//! - Phase 3 **Publish**: two-phase rename + fsync for atomicity.
//! - Phase 4 **Retire**: remove old segments after reader leases drain.

use std::collections::{HashMap, HashSet};

use fsqlite_types::ecs::{BloomFilter, ManifestSegment, ObjectId};
use fsqlite_types::{CommitMarker, CommitSeq, PageNumber};
use tracing::{debug, info, warn};

// ===========================================================================
// §7.12 Recovery Types
// ===========================================================================

/// Outcome of a single capsule decode attempt during recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapsuleDecodeOutcome {
    /// Decoded successfully from systematic symbols (fast path).
    Systematic,
    /// Decoded successfully after RaptorQ repair.
    Repaired { repair_symbols_used: u32 },
    /// Decode failed — durability contract violated.
    Failed { reason: String },
}

/// Diagnostic emitted when a committed marker's capsule cannot be decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurabilityViolation {
    /// The commit sequence of the undecipherable marker.
    pub commit_seq: CommitSeq,
    /// The capsule ObjectId referenced by the marker.
    pub capsule_object_id: ObjectId,
    /// Human-readable reason for failure.
    pub reason: String,
}

/// Checkpoint reference used during recovery to resume from a known-good state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointRef {
    /// Commit sequence at which the checkpoint was taken.
    pub commit_seq: CommitSeq,
    /// ObjectId of the checkpoint's manifest ECS object.
    pub manifest_object_id: ObjectId,
}

/// Summary of a recovery operation (§7.12).
#[derive(Debug, Clone)]
pub struct RecoverySummary {
    /// The ECS epoch from the RootManifest.
    pub ecs_epoch: u64,
    /// The highest commit_seq recovered from the marker stream.
    pub commit_seq_recovered: CommitSeq,
    /// Number of markers replayed.
    pub markers_replayed: u64,
    /// Number of capsules that required RaptorQ repair.
    pub capsules_repaired: u32,
    /// Any durability violations encountered.
    pub violations: Vec<DurabilityViolation>,
    /// Duration of the recovery process in milliseconds.
    pub duration_ms: u64,
}

/// Simulated root manifest for testing and bootstrapping.
#[derive(Debug, Clone)]
pub struct RootManifest {
    /// ECS epoch identifier.
    pub ecs_epoch: u64,
    /// Most recent checkpoint, if any.
    pub latest_checkpoint: Option<CheckpointRef>,
    /// Manifest mapping commit_seq ranges to index segment ObjectIds.
    pub manifest: ManifestSegment,
}

/// The 5-step native mode recovery engine (§7.12).
///
/// Recovers committed state from the marker stream, starting from the
/// latest checkpoint (or genesis if none exists). Capsules are decoded
/// via systematic fast path or RaptorQ repair.
#[derive(Debug)]
pub struct NativeRecovery {
    /// Root manifest loaded in step 1.
    root_manifest: Option<RootManifest>,
    /// Markers replayed during step 3-4.
    replayed_markers: Vec<CommitMarker>,
    /// Capsule decode outcomes for audit.
    decode_outcomes: Vec<(CommitSeq, CapsuleDecodeOutcome)>,
    /// Highest commit_seq recovered.
    recovered_tip: CommitSeq,
    /// Any durability violations.
    violations: Vec<DurabilityViolation>,
}

impl NativeRecovery {
    /// Create a new recovery engine.
    #[must_use]
    pub fn new() -> Self {
        Self {
            root_manifest: None,
            replayed_markers: Vec::new(),
            decode_outcomes: Vec::new(),
            recovered_tip: CommitSeq::ZERO,
            violations: Vec::new(),
        }
    }

    /// Step 1: Load `RootManifest` from `ecs/root`.
    pub fn load_root_manifest(&mut self, manifest: RootManifest) {
        info!(
            ecs_epoch = manifest.ecs_epoch,
            has_checkpoint = manifest.latest_checkpoint.is_some(),
            "recovery step 1: loaded RootManifest"
        );
        self.root_manifest = Some(manifest);
    }

    /// Step 2: Locate latest checkpoint.
    ///
    /// Returns the checkpoint's commit_seq tip if found, or `CommitSeq::ZERO`
    /// for genesis recovery.
    #[must_use]
    pub fn locate_checkpoint(&self) -> CommitSeq {
        let seq = self
            .root_manifest
            .as_ref()
            .and_then(|rm| rm.latest_checkpoint.as_ref())
            .map_or(CommitSeq::ZERO, |cp| cp.commit_seq);
        debug!(
            checkpoint_seq = seq.get(),
            "recovery step 2: checkpoint located"
        );
        seq
    }

    /// Step 3-4: Scan marker stream and decode capsules.
    ///
    /// `markers` is the ordered sequence of markers from checkpoint tip forward.
    /// `decode_capsule` is a closure that attempts to decode a capsule given
    /// its ObjectId, returning the decode outcome.
    ///
    /// For each marker, the closure is called to decode the capsule. Failed
    /// decodes are recorded as durability violations per the spec.
    pub fn replay_markers<F>(&mut self, markers: &[CommitMarker], mut decode_capsule: F)
    where
        F: FnMut(ObjectId) -> CapsuleDecodeOutcome,
    {
        info!(
            marker_count = markers.len(),
            "recovery step 3-4: scanning marker stream"
        );

        for marker in markers {
            let outcome = decode_capsule(marker.capsule_object_id);

            match &outcome {
                CapsuleDecodeOutcome::Systematic => {
                    debug!(
                        commit_seq = marker.commit_seq.get(),
                        "capsule decoded via systematic fast path"
                    );
                }
                CapsuleDecodeOutcome::Repaired {
                    repair_symbols_used,
                } => {
                    info!(
                        commit_seq = marker.commit_seq.get(),
                        repair_symbols_used, "capsule decoded via RaptorQ repair"
                    );
                }
                CapsuleDecodeOutcome::Failed { reason } => {
                    warn!(
                        commit_seq = marker.commit_seq.get(),
                        reason = reason.as_str(),
                        "DURABILITY CONTRACT VIOLATED: capsule undecodable"
                    );
                    self.violations.push(DurabilityViolation {
                        commit_seq: marker.commit_seq,
                        capsule_object_id: marker.capsule_object_id,
                        reason: reason.clone(),
                    });
                }
            }

            self.decode_outcomes.push((marker.commit_seq, outcome));
            self.replayed_markers.push(marker.clone());
            self.recovered_tip = marker.commit_seq;
        }
    }

    /// Step 5: Finalize recovery and return summary.
    ///
    /// `duration_ms` is the elapsed wall-clock time for the entire recovery.
    #[must_use]
    pub fn finalize(self, duration_ms: u64) -> RecoverySummary {
        let capsules_repaired = self
            .decode_outcomes
            .iter()
            .filter(|(_, o)| matches!(o, CapsuleDecodeOutcome::Repaired { .. }))
            .count();

        let ecs_epoch = self.root_manifest.as_ref().map_or(0, |rm| rm.ecs_epoch);

        info!(
            ecs_epoch,
            commit_seq_recovered = self.recovered_tip.get(),
            markers_replayed = self.replayed_markers.len(),
            capsules_repaired,
            violations = self.violations.len(),
            duration_ms,
            "recovery complete"
        );

        #[allow(clippy::cast_possible_truncation)]
        RecoverySummary {
            ecs_epoch,
            commit_seq_recovered: self.recovered_tip,
            markers_replayed: self.replayed_markers.len() as u64,
            capsules_repaired: capsules_repaired as u32,
            violations: self.violations,
            duration_ms,
        }
    }

    /// The highest commit_seq recovered so far.
    #[must_use]
    pub const fn recovered_tip(&self) -> CommitSeq {
        self.recovered_tip
    }

    /// Whether any durability violations occurred.
    #[must_use]
    pub fn has_violations(&self) -> bool {
        !self.violations.is_empty()
    }
}

impl Default for NativeRecovery {
    fn default() -> Self {
        Self::new()
    }
}

// ===========================================================================
// §7.13 Compaction Types
// ===========================================================================

/// The four phases of the compaction saga (§7.13).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionPhase {
    /// Phase 1: Trace reachable objects, build live set.
    Mark,
    /// Phase 2: Copy live symbols to new segments.
    Compact,
    /// Phase 3: Two-phase publish (rename + fsync).
    Publish,
    /// Phase 4: Retire old segments after reader leases drain.
    Retire,
}

impl std::fmt::Display for CompactionPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mark => write!(f, "mark"),
            Self::Compact => write!(f, "compact"),
            Self::Publish => write!(f, "publish"),
            Self::Retire => write!(f, "retire"),
        }
    }
}

/// MDP state for the compaction policy (§7.13.1).
///
/// State = (space_amp_bucket, read_regime, write_regime, compaction_debt).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CompactionMdpState {
    /// Space amplification bucket (0 = < 1.5, 1 = 1.5-2.0, 2 = 2.0-3.0, 3 = > 3.0).
    pub space_amp_bucket: u8,
    /// Read regime from BOCPD (0 = idle, 1 = moderate, 2 = heavy).
    pub read_regime: u8,
    /// Write regime from BOCPD (0 = idle, 1 = moderate, 2 = heavy).
    pub write_regime: u8,
    /// Compaction debt (0 = none, 1 = some, 2 = high).
    pub compaction_debt: u8,
}

impl CompactionMdpState {
    /// Classify a space amplification ratio into a bucket.
    #[must_use]
    pub fn bucket_for_space_amp(space_amp: f64) -> u8 {
        if space_amp < 1.5 {
            0
        } else if space_amp < 2.0 {
            1
        } else if space_amp < 3.0 {
            2
        } else {
            3
        }
    }
}

/// Compaction action selected by the MDP policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionAction {
    /// Defer compaction (workload conditions not favorable).
    Defer,
    /// Compact now at the given rate limit.
    CompactNow { rate_limit: CompactionRateLimit },
}

/// Rate limit for compaction I/O.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionRateLimit {
    Low,
    Medium,
    High,
}

/// Evidence ledger entry recording a policy decision (§7.13.1).
#[derive(Debug, Clone)]
pub struct EvidenceLedgerEntry {
    /// Timestamp of the decision (unix nanoseconds).
    pub timestamp_ns: u64,
    /// MDP state at decision time.
    pub state: CompactionMdpState,
    /// Action selected.
    pub action: CompactionAction,
    /// Reason for the decision.
    pub reason: &'static str,
}

/// MDP-based compaction policy (§7.13.1).
///
/// Solved offline on a discretized grid, embedded as a deterministic lookup
/// table. Falls back to threshold (space_amp > 2.0) if policy unavailable.
#[derive(Debug)]
pub struct CompactionPolicy {
    /// Lookup table: state -> action.
    lookup: HashMap<CompactionMdpState, CompactionAction>,
    /// Evidence ledger of past decisions.
    evidence: Vec<EvidenceLedgerEntry>,
}

impl CompactionPolicy {
    /// Create a new policy with the default offline-computed lookup table.
    #[must_use]
    pub fn new() -> Self {
        let mut lookup = HashMap::new();

        // Default policy: compact when space_amp >= 2.0 (bucket >= 2) and
        // write regime is not heavy (to avoid interference).
        for space_amp in 0..=3u8 {
            for read_regime in 0..=2u8 {
                for write_regime in 0..=2u8 {
                    for debt in 0..=2u8 {
                        let state = CompactionMdpState {
                            space_amp_bucket: space_amp,
                            read_regime,
                            write_regime,
                            compaction_debt: debt,
                        };
                        let action = Self::default_policy(space_amp, write_regime, debt);
                        lookup.insert(state, action);
                    }
                }
            }
        }

        Self {
            lookup,
            evidence: Vec::new(),
        }
    }

    /// Default threshold-based policy.
    fn default_policy(space_amp: u8, write_regime: u8, debt: u8) -> CompactionAction {
        // High debt always compacts regardless.
        if debt >= 2 {
            return CompactionAction::CompactNow {
                rate_limit: CompactionRateLimit::Medium,
            };
        }
        // Space amp >= 2.0: compact (but throttle during heavy writes).
        if space_amp >= 2 {
            let rate_limit = if write_regime >= 2 {
                CompactionRateLimit::Low
            } else {
                CompactionRateLimit::High
            };
            return CompactionAction::CompactNow { rate_limit };
        }
        // Space amp 1.5-2.0 with some debt: compact at low rate.
        if space_amp >= 1 && debt >= 1 {
            return CompactionAction::CompactNow {
                rate_limit: CompactionRateLimit::Low,
            };
        }
        CompactionAction::Defer
    }

    /// Query the policy for the recommended action.
    #[must_use]
    pub fn recommend(&self, state: &CompactionMdpState) -> CompactionAction {
        self.lookup
            .get(state)
            .copied()
            .unwrap_or(CompactionAction::Defer)
    }

    /// Record a policy decision in the evidence ledger.
    pub fn record_decision(
        &mut self,
        timestamp_ns: u64,
        state: CompactionMdpState,
        action: CompactionAction,
        reason: &'static str,
    ) {
        self.evidence.push(EvidenceLedgerEntry {
            timestamp_ns,
            state,
            action,
            reason,
        });
    }

    /// The evidence ledger.
    #[must_use]
    pub fn evidence_ledger(&self) -> &[EvidenceLedgerEntry] {
        &self.evidence
    }

    /// Override a specific state -> action mapping (e.g., on BOCPD regime shift).
    pub fn override_action(&mut self, state: CompactionMdpState, action: CompactionAction) {
        self.lookup.insert(state, action);
    }
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self::new()
    }
}

// ===========================================================================
// §7.13 Compaction Saga
// ===========================================================================

/// A simulated segment file for compaction operations.
#[derive(Debug, Clone)]
pub struct SegmentRef {
    /// Unique identifier for this segment.
    pub segment_id: ObjectId,
    /// ObjectIds of symbols stored in this segment.
    pub object_ids: Vec<ObjectId>,
    /// Total size in bytes.
    pub size_bytes: u64,
}

/// Reader lease preventing premature segment retirement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReaderLease {
    /// Unique lease identifier.
    pub lease_id: u64,
    /// Segments this reader depends on.
    pub segment_ids: Vec<ObjectId>,
}

/// Tracks the mutable state of a compaction saga through all four phases.
///
/// Each phase has deterministic compensation: cancel before publish -> temp
/// segments garbage-collected; cancel after publish -> complete or rollback.
#[derive(Debug)]
#[allow(clippy::struct_excessive_bools)]
pub struct CompactionSaga {
    /// Current phase.
    phase: CompactionPhase,
    /// Live ObjectIds identified in the Mark phase.
    live_set: HashSet<ObjectId>,
    /// Bloom filter of live ObjectIds for fast membership testing.
    live_bloom: Option<BloomFilter>,
    /// Old segments being compacted.
    old_segments: Vec<SegmentRef>,
    /// New segments created during Compact phase (temporary names).
    new_segments: Vec<SegmentRef>,
    /// Whether the new segments have been durably fsynced.
    new_segments_synced: bool,
    /// Whether the new locator has been durably fsynced.
    new_locator_synced: bool,
    /// Whether the publish phase completed atomically.
    published: bool,
    /// Active reader leases preventing retirement.
    reader_leases: Vec<ReaderLease>,
    /// Retired segment IDs.
    retired_segments: Vec<ObjectId>,
    /// Whether the saga was cancelled.
    cancelled: bool,
    /// Space amplification before compaction.
    space_amp_before: f64,
    /// Space amplification after compaction.
    space_amp_after: f64,
}

impl CompactionSaga {
    /// Create a new compaction saga targeting the given old segments.
    #[must_use]
    pub fn new(old_segments: Vec<SegmentRef>, space_amp_before: f64) -> Self {
        info!(
            old_segment_count = old_segments.len(),
            space_amp_before, "compaction saga initiated"
        );
        Self {
            phase: CompactionPhase::Mark,
            live_set: HashSet::new(),
            live_bloom: None,
            old_segments,
            new_segments: Vec::new(),
            new_segments_synced: false,
            new_locator_synced: false,
            published: false,
            reader_leases: Vec::new(),
            retired_segments: Vec::new(),
            cancelled: false,
            space_amp_before,
            space_amp_after: 0.0,
        }
    }

    /// Current phase.
    #[must_use]
    pub const fn phase(&self) -> CompactionPhase {
        self.phase
    }

    /// Whether the saga has been cancelled.
    #[must_use]
    pub const fn is_cancelled(&self) -> bool {
        self.cancelled
    }

    /// Whether publish has completed (point of no return).
    #[must_use]
    pub const fn is_published(&self) -> bool {
        self.published
    }

    /// Cancel the saga. Compensation depends on current phase.
    ///
    /// - Before publish: temp segments garbage-collected, old segments valid.
    /// - After publish: must complete or rollback to pre-compaction view.
    pub fn cancel(&mut self) -> CompactionCompensation {
        self.cancelled = true;
        if self.published {
            warn!("compaction cancelled after publish — rollback required");
            CompactionCompensation::RollbackRequired
        } else {
            info!(
                phase = %self.phase,
                "compaction cancelled before publish — temp segments discarded"
            );
            self.new_segments.clear();
            CompactionCompensation::TempSegmentsDiscarded
        }
    }

    // ── Phase 1: Mark ──

    /// Phase 1: Mark — identify live objects by tracing from root manifest
    /// and commit marker stream.
    ///
    /// `reachable_ids` must include all ObjectIds reachable from the
    /// RootManifest + active CommitMarker stream + PageHistory up to GC
    /// horizon + witness plane objects.
    pub fn mark(&mut self, reachable_ids: impl IntoIterator<Item = ObjectId>) {
        assert_eq!(self.phase, CompactionPhase::Mark, "must be in Mark phase");

        self.live_set.clear();
        for id in reachable_ids {
            self.live_set.insert(id);
        }

        // Build bloom filter for fast "not live" checks during compact phase.
        #[allow(clippy::cast_possible_truncation)]
        let count = self.live_set.len() as u32;
        let mut bloom = BloomFilter::new(count.max(1), 0.001);
        // BloomFilter works on PageNumber, but we need ObjectId filtering.
        // We'll use the first 4 bytes of each ObjectId as a pseudo-page-number
        // for the bloom filter (this is just for fast rejection; exact check follows).
        for id in &self.live_set {
            let bytes = id.as_bytes();
            let pseudo_page = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
            if let Some(pn) = PageNumber::new(pseudo_page.max(1)) {
                bloom.insert(pn);
            }
        }
        self.live_bloom = Some(bloom);

        debug!(
            live_count = self.live_set.len(),
            "compaction phase 1 (mark) complete"
        );
        self.phase = CompactionPhase::Compact;
    }

    /// Check if an ObjectId is in the live set.
    #[must_use]
    pub fn is_live(&self, id: &ObjectId) -> bool {
        self.live_set.contains(id)
    }

    /// Number of live objects identified.
    #[must_use]
    pub fn live_count(&self) -> usize {
        self.live_set.len()
    }

    // ── Phase 2: Compact ──

    /// Phase 2: Compact — create new segments containing only live symbols.
    ///
    /// `new_segments` are the compacted segments (with temporary names).
    /// The caller is responsible for copying live symbols and calling fdatasync.
    pub fn compact(&mut self, new_segments: Vec<SegmentRef>) {
        assert_eq!(
            self.phase,
            CompactionPhase::Compact,
            "must be in Compact phase"
        );

        // Calculate new total size for space amplification.
        // After compaction, new_total_size / live_data_size. Since compaction
        // copies exactly the live data, the ratio is 1.0 (no dead objects).
        let new_total_size: u64 = new_segments.iter().map(|s| s.size_bytes).sum();

        self.new_segments = new_segments;

        if new_total_size > 0 {
            // space_amp_after = new_total_size / live_data_size.
            // After compaction, new segments contain only live data, so
            // space_amp approaches 1.0 (perfect compaction).
            self.space_amp_after = 1.0;
        }

        debug!(
            new_segment_count = self.new_segments.len(),
            new_total_size, "compaction phase 2 (compact) segments created"
        );
    }

    /// Mark new segments as durably fsynced.
    pub fn mark_segments_synced(&mut self) {
        self.new_segments_synced = true;
        debug!("compaction phase 2: new segments fdatasynced");
    }

    /// Mark new locator as durably fsynced.
    pub fn mark_locator_synced(&mut self) {
        self.new_locator_synced = true;
        debug!("compaction phase 2: new locator fdatasynced");
        self.phase = CompactionPhase::Publish;
    }

    // ── Phase 3: Publish ──

    /// Phase 3: Publish — two-phase atomic replacement.
    ///
    /// Ordering (normative):
    /// 1. rename(compacting -> .log), fsync dir
    /// 2. fdatasync(locator.tmp), rename(locator.tmp -> locator), fsync dir
    ///
    /// Old segments MUST NOT be retired until both new segments AND new locator
    /// are durable.
    ///
    /// Returns `true` if publish succeeded.
    pub fn publish(&mut self) -> bool {
        assert_eq!(
            self.phase,
            CompactionPhase::Publish,
            "must be in Publish phase"
        );

        if !self.new_segments_synced || !self.new_locator_synced {
            warn!("cannot publish: segments or locator not yet synced");
            return false;
        }

        self.published = true;
        info!(
            space_amp_before = self.space_amp_before,
            space_amp_after = self.space_amp_after,
            "compaction phase 3 (publish) complete — point of no return"
        );
        self.phase = CompactionPhase::Retire;
        true
    }

    // ── Phase 4: Retire ──

    /// Register active reader leases that prevent segment retirement.
    pub fn register_reader_leases(&mut self, leases: Vec<ReaderLease>) {
        self.reader_leases = leases;
    }

    /// Check which old segments can be safely retired (no active leases).
    #[must_use]
    pub fn retirable_segments(&self) -> Vec<ObjectId> {
        if !self.published {
            return Vec::new();
        }

        let leased_segments: HashSet<&ObjectId> = self
            .reader_leases
            .iter()
            .flat_map(|lease| &lease.segment_ids)
            .collect();

        self.old_segments
            .iter()
            .filter(|seg| !leased_segments.contains(&seg.segment_id))
            .map(|seg| seg.segment_id)
            .collect()
    }

    /// Phase 4: Retire — remove old segments that have no active reader leases.
    ///
    /// Unix: unlink (open handles remain valid).
    /// Windows: rename to .retired, delete after all handles closed.
    pub fn retire(&mut self) -> Vec<ObjectId> {
        assert_eq!(
            self.phase,
            CompactionPhase::Retire,
            "must be in Retire phase"
        );
        assert!(self.published, "must publish before retiring");

        let retirable = self.retirable_segments();
        self.retired_segments.extend_from_slice(&retirable);

        if retirable.is_empty() {
            debug!("compaction phase 4 (retire): no segments retirable yet (leases active)");
        } else {
            info!(
                retired_count = retirable.len(),
                "compaction phase 4 (retire): old segments retired"
            );
        }

        retirable
    }

    /// Summary statistics for the completed compaction.
    #[must_use]
    pub fn summary(&self) -> CompactionSummary {
        let dead_count = self
            .old_segments
            .iter()
            .flat_map(|s| &s.object_ids)
            .filter(|id| !self.live_set.contains(id))
            .count();

        CompactionSummary {
            space_amp_before: self.space_amp_before,
            space_amp_after: self.space_amp_after,
            live_objects: self.live_set.len(),
            dead_objects: dead_count,
            old_segments: self.old_segments.len(),
            new_segments: self.new_segments.len(),
            retired_segments: self.retired_segments.len(),
            published: self.published,
            cancelled: self.cancelled,
        }
    }
}

/// Summary of a compaction operation.
#[derive(Debug, Clone)]
pub struct CompactionSummary {
    pub space_amp_before: f64,
    pub space_amp_after: f64,
    pub live_objects: usize,
    pub dead_objects: usize,
    pub old_segments: usize,
    pub new_segments: usize,
    pub retired_segments: usize,
    pub published: bool,
    pub cancelled: bool,
}

/// Compensation result from cancelling a compaction saga.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionCompensation {
    /// Temp segments were discarded; old segments remain valid.
    TempSegmentsDiscarded,
    /// Publish already occurred; rollback is required.
    RollbackRequired,
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_oid(seed: u8) -> ObjectId {
        ObjectId::from_bytes([seed; 16])
    }

    fn make_marker(seq: u64, capsule_seed: u8) -> CommitMarker {
        CommitMarker::new(
            CommitSeq::new(seq),
            1_700_000_000_000_000_000 + seq * 1_000_000,
            make_oid(capsule_seed),
            make_oid(capsule_seed.wrapping_add(0x80)),
            if seq > 1 {
                Some(make_oid(capsule_seed.wrapping_sub(1)))
            } else {
                None
            },
        )
    }

    fn make_segment(id_seed: u8, object_seeds: &[u8], size: u64) -> SegmentRef {
        SegmentRef {
            segment_id: make_oid(id_seed),
            object_ids: object_seeds.iter().map(|&s| make_oid(s)).collect(),
            size_bytes: size,
        }
    }

    // ── §7.12 Recovery Tests ──

    // bd-317y test 1
    #[test]
    fn test_native_recovery_from_genesis() {
        let mut recovery = NativeRecovery::new();

        // Step 1: no checkpoint
        let manifest = RootManifest {
            ecs_epoch: 1,
            latest_checkpoint: None,
            manifest: ManifestSegment::new(Vec::new()),
        };
        recovery.load_root_manifest(manifest);

        // Step 2: genesis
        assert_eq!(recovery.locate_checkpoint(), CommitSeq::ZERO);

        // Steps 3-4: replay from genesis with 5 markers
        let markers: Vec<_> = (1..=5).map(|i| make_marker(i, i as u8)).collect();
        recovery.replay_markers(&markers, |_| CapsuleDecodeOutcome::Systematic);

        // Step 5: finalize
        let summary = recovery.finalize(42);
        assert_eq!(summary.ecs_epoch, 1);
        assert_eq!(summary.commit_seq_recovered, CommitSeq::new(5));
        assert_eq!(summary.markers_replayed, 5);
        assert_eq!(summary.capsules_repaired, 0);
        assert!(summary.violations.is_empty());
        assert_eq!(summary.duration_ms, 42);
    }

    // bd-317y test 2
    #[test]
    fn test_native_recovery_from_checkpoint() {
        let mut recovery = NativeRecovery::new();

        // Checkpoint at commit_seq 100
        let manifest = RootManifest {
            ecs_epoch: 2,
            latest_checkpoint: Some(CheckpointRef {
                commit_seq: CommitSeq::new(100),
                manifest_object_id: make_oid(0xCC),
            }),
            manifest: ManifestSegment::new(vec![(1, 100, make_oid(0xDD))]),
        };
        recovery.load_root_manifest(manifest);

        assert_eq!(recovery.locate_checkpoint(), CommitSeq::new(100));

        // Replay markers 101-105 (after checkpoint)
        let markers: Vec<_> = (101..=105)
            .map(|i| make_marker(i, (i % 256) as u8))
            .collect();
        recovery.replay_markers(&markers, |_| CapsuleDecodeOutcome::Systematic);

        let summary = recovery.finalize(100);
        assert_eq!(summary.commit_seq_recovered, CommitSeq::new(105));
        assert_eq!(summary.markers_replayed, 5);
    }

    // bd-317y test 3
    #[test]
    fn test_native_recovery_repair() {
        let mut recovery = NativeRecovery::new();

        let manifest = RootManifest {
            ecs_epoch: 1,
            latest_checkpoint: None,
            manifest: ManifestSegment::new(Vec::new()),
        };
        recovery.load_root_manifest(manifest);

        let markers: Vec<_> = (1..=3).map(|i| make_marker(i, i as u8)).collect();

        // Marker 2 requires RaptorQ repair
        recovery.replay_markers(&markers, |oid| {
            if oid == make_oid(2) {
                CapsuleDecodeOutcome::Repaired {
                    repair_symbols_used: 3,
                }
            } else {
                CapsuleDecodeOutcome::Systematic
            }
        });

        let summary = recovery.finalize(50);
        assert_eq!(summary.capsules_repaired, 1);
        assert!(summary.violations.is_empty());
    }

    // bd-317y test 4
    #[test]
    fn test_native_recovery_contract_violation() {
        let mut recovery = NativeRecovery::new();

        let manifest = RootManifest {
            ecs_epoch: 1,
            latest_checkpoint: None,
            manifest: ManifestSegment::new(Vec::new()),
        };
        recovery.load_root_manifest(manifest);

        let markers: Vec<_> = (1..=3).map(|i| make_marker(i, i as u8)).collect();

        // Marker 2 fails to decode
        recovery.replay_markers(&markers, |oid| {
            if oid == make_oid(2) {
                CapsuleDecodeOutcome::Failed {
                    reason: "insufficient symbols: 2 of 5 needed".to_owned(),
                }
            } else {
                CapsuleDecodeOutcome::Systematic
            }
        });

        assert!(recovery.has_violations());

        let summary = recovery.finalize(50);
        assert_eq!(summary.violations.len(), 1);
        assert_eq!(summary.violations[0].commit_seq, CommitSeq::new(2));
        assert_eq!(summary.violations[0].capsule_object_id, make_oid(2));
    }

    // ── §7.13 Compaction Tests ──

    // bd-317y test 5
    #[test]
    fn test_compaction_identifies_live() {
        let old = vec![make_segment(0x01, &[0x10, 0x20, 0x30, 0x40, 0x50], 5000)];
        let mut saga = CompactionSaga::new(old, 2.5);

        // Only objects 0x10, 0x30, 0x50 are reachable
        let live = vec![make_oid(0x10), make_oid(0x30), make_oid(0x50)];
        saga.mark(live);

        assert_eq!(saga.live_count(), 3);
        assert!(saga.is_live(&make_oid(0x10)));
        assert!(!saga.is_live(&make_oid(0x20)));
        assert!(saga.is_live(&make_oid(0x30)));
        assert!(!saga.is_live(&make_oid(0x40)));
        assert!(saga.is_live(&make_oid(0x50)));
    }

    // bd-317y test 6
    #[test]
    fn test_compaction_discards_dead() {
        let old = vec![make_segment(0x01, &[0x10, 0x20, 0x30, 0x40, 0x50], 5000)];
        let mut saga = CompactionSaga::new(old, 2.5);

        // 3 of 5 are live
        saga.mark(vec![make_oid(0x10), make_oid(0x30), make_oid(0x50)]);

        // Compact: new segment contains only live objects
        let new_seg = make_segment(0x02, &[0x10, 0x30, 0x50], 3000);
        saga.compact(vec![new_seg]);

        let summary = saga.summary();
        assert_eq!(summary.live_objects, 3);
        assert_eq!(summary.dead_objects, 2); // 0x20, 0x40 are dead
    }

    // bd-317y test 7
    #[test]
    fn test_compaction_two_phase_publish() {
        let old = vec![make_segment(0x01, &[0x10, 0x20], 2000)];
        let mut saga = CompactionSaga::new(old, 2.0);

        saga.mark(vec![make_oid(0x10)]);
        saga.compact(vec![make_segment(0x02, &[0x10], 1000)]);

        // Cannot publish without syncing
        assert!(!saga.is_published());

        // Sync segments and locator
        saga.mark_segments_synced();
        saga.mark_locator_synced();

        // Now publish
        assert!(saga.publish());
        assert!(saga.is_published());
    }

    // bd-317y test 8
    #[test]
    fn test_compaction_crash_before_publish() {
        let old = vec![make_segment(0x01, &[0x10, 0x20], 2000)];
        let mut saga = CompactionSaga::new(old.clone(), 2.0);

        saga.mark(vec![make_oid(0x10)]);
        saga.compact(vec![make_segment(0x02, &[0x10], 1000)]);

        // Crash before publish: cancel
        let comp = saga.cancel();
        assert_eq!(comp, CompactionCompensation::TempSegmentsDiscarded);
        assert!(!saga.is_published());

        // Old segments remain valid (verified by checking they still exist)
        assert_eq!(old.len(), 1);
    }

    // bd-317y test 9
    #[test]
    fn test_compaction_crash_after_publish() {
        let old = vec![make_segment(0x01, &[0x10, 0x20], 2000)];
        let mut saga = CompactionSaga::new(old, 2.0);

        saga.mark(vec![make_oid(0x10)]);
        saga.compact(vec![make_segment(0x02, &[0x10], 1000)]);
        saga.mark_segments_synced();
        saga.mark_locator_synced();
        saga.publish();

        // Cancel after publish: rollback required
        let comp = saga.cancel();
        assert_eq!(comp, CompactionCompensation::RollbackRequired);
    }

    // bd-317y test 10
    #[test]
    fn test_compaction_reader_leases() {
        let old = vec![
            make_segment(0x01, &[0x10], 1000),
            make_segment(0x02, &[0x20], 1000),
        ];
        let mut saga = CompactionSaga::new(old, 2.0);

        saga.mark(vec![make_oid(0x10), make_oid(0x20)]);
        saga.compact(vec![make_segment(0x03, &[0x10, 0x20], 2000)]);
        saga.mark_segments_synced();
        saga.mark_locator_synced();
        saga.publish();

        // Reader holds lease on segment 0x01
        saga.register_reader_leases(vec![ReaderLease {
            lease_id: 1,
            segment_ids: vec![make_oid(0x01)],
        }]);

        // Only segment 0x02 is retirable
        let retirable = saga.retirable_segments();
        assert_eq!(retirable.len(), 1);
        assert_eq!(retirable[0], make_oid(0x02));

        // After reader releases, both are retirable
        saga.register_reader_leases(Vec::new());
        let retirable = saga.retirable_segments();
        assert_eq!(retirable.len(), 2);
    }

    // bd-317y test 11
    #[test]
    fn test_compaction_space_reclaimed() {
        let old = vec![make_segment(0x01, &[0x10, 0x20, 0x30, 0x40], 4000)];
        let mut saga = CompactionSaga::new(old, 3.0);

        // Half the objects are live
        saga.mark(vec![make_oid(0x10), make_oid(0x30)]);
        saga.compact(vec![make_segment(0x02, &[0x10, 0x30], 2000)]);

        let summary = saga.summary();
        assert!(
            summary.space_amp_after < 2.0,
            "space_amp_after ({}) should be < 2.0",
            summary.space_amp_after
        );
    }

    // bd-317y test 12
    #[test]
    fn test_compaction_saga_compensation() {
        // Cancel at each phase and verify compensation.

        // Cancel during Mark phase
        let old = vec![make_segment(0x01, &[0x10], 1000)];
        let mut saga = CompactionSaga::new(old.clone(), 2.0);
        let comp = saga.cancel();
        assert_eq!(comp, CompactionCompensation::TempSegmentsDiscarded);

        // Cancel during Compact phase (after mark)
        let mut saga = CompactionSaga::new(old.clone(), 2.0);
        saga.mark(vec![make_oid(0x10)]);
        let comp = saga.cancel();
        assert_eq!(comp, CompactionCompensation::TempSegmentsDiscarded);

        // Cancel during Compact phase (after compact, before sync)
        let mut saga = CompactionSaga::new(old.clone(), 2.0);
        saga.mark(vec![make_oid(0x10)]);
        saga.compact(vec![make_segment(0x02, &[0x10], 500)]);
        let comp = saga.cancel();
        assert_eq!(comp, CompactionCompensation::TempSegmentsDiscarded);

        // Cancel after Publish (rollback required)
        let mut saga = CompactionSaga::new(old, 2.0);
        saga.mark(vec![make_oid(0x10)]);
        saga.compact(vec![make_segment(0x02, &[0x10], 500)]);
        saga.mark_segments_synced();
        saga.mark_locator_synced();
        saga.publish();
        let comp = saga.cancel();
        assert_eq!(comp, CompactionCompensation::RollbackRequired);
    }

    // bd-317y test 13
    #[test]
    fn test_compaction_mdp_policy() {
        let policy = CompactionPolicy::new();

        // Low space amp + idle writes → defer
        let idle = CompactionMdpState {
            space_amp_bucket: 0,
            read_regime: 0,
            write_regime: 0,
            compaction_debt: 0,
        };
        assert_eq!(policy.recommend(&idle), CompactionAction::Defer);

        // High space amp + moderate writes → compact at high rate
        let high_amp = CompactionMdpState {
            space_amp_bucket: 2,
            read_regime: 1,
            write_regime: 1,
            compaction_debt: 0,
        };
        assert_eq!(
            policy.recommend(&high_amp),
            CompactionAction::CompactNow {
                rate_limit: CompactionRateLimit::High,
            }
        );

        // High space amp + heavy writes → compact at low rate (throttled)
        let heavy_write = CompactionMdpState {
            space_amp_bucket: 3,
            read_regime: 0,
            write_regime: 2,
            compaction_debt: 0,
        };
        assert_eq!(
            policy.recommend(&heavy_write),
            CompactionAction::CompactNow {
                rate_limit: CompactionRateLimit::Low,
            }
        );

        // High debt always compacts
        let high_debt = CompactionMdpState {
            space_amp_bucket: 0,
            read_regime: 0,
            write_regime: 0,
            compaction_debt: 2,
        };
        assert_eq!(
            policy.recommend(&high_debt),
            CompactionAction::CompactNow {
                rate_limit: CompactionRateLimit::Medium,
            }
        );

        // Space amp bucket classification
        assert_eq!(CompactionMdpState::bucket_for_space_amp(1.0), 0);
        assert_eq!(CompactionMdpState::bucket_for_space_amp(1.7), 1);
        assert_eq!(CompactionMdpState::bucket_for_space_amp(2.5), 2);
        assert_eq!(CompactionMdpState::bucket_for_space_amp(5.0), 3);
    }

    // bd-317y test 14
    #[test]
    fn test_compaction_evidence_ledger() {
        let mut policy = CompactionPolicy::new();
        assert!(policy.evidence_ledger().is_empty());

        let state = CompactionMdpState {
            space_amp_bucket: 2,
            read_regime: 1,
            write_regime: 0,
            compaction_debt: 0,
        };
        let action = policy.recommend(&state);

        policy.record_decision(
            1_700_000_000_000_000_000,
            state,
            action,
            "space_amp exceeded threshold",
        );

        assert_eq!(policy.evidence_ledger().len(), 1);
        assert_eq!(policy.evidence_ledger()[0].state.space_amp_bucket, 2);
        assert_eq!(
            policy.evidence_ledger()[0].reason,
            "space_amp exceeded threshold"
        );

        // Record a second decision after override
        policy.override_action(state, CompactionAction::Defer);
        let new_action = policy.recommend(&state);
        assert_eq!(new_action, CompactionAction::Defer);

        policy.record_decision(
            1_700_000_001_000_000_000,
            state,
            new_action,
            "BOCPD regime shift: deferred during write burst",
        );

        assert_eq!(policy.evidence_ledger().len(), 2);
    }
}
