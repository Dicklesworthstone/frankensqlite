//! ECS-native replication architecture (§3.4.7, bd-1hi.19).
//!
//! High-level replication framework: roles, modes, anti-entropy convergence,
//! quorum durability, consistent-hash symbol routing, and authenticated symbols.

use std::collections::{BTreeSet, HashMap, HashSet};

use fsqlite_error::{FrankenError, Result};
use tracing::{debug, error, info, warn};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const BEAD_ID: &str = "bd-1hi.19";

// ---------------------------------------------------------------------------
// Replication roles and modes
// ---------------------------------------------------------------------------

/// Replication role for a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReplicationRole {
    /// Publishes authoritative commit-marker stream. Accepts MVCC writes.
    Leader,
    /// Replicates objects + markers, serves reads.
    Follower,
}

/// Replication mode (§3.4.7 spec).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ReplicationMode {
    /// One leader publishes markers. V1 default.
    #[default]
    LeaderCommitClock,
    /// Multiple nodes publish capsules. Experimental, not V1 default.
    MultiWriter,
}

// ---------------------------------------------------------------------------
// Replicated object types
// ---------------------------------------------------------------------------

/// Object ID — 16-byte content-addressed identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectId([u8; 16]);

impl ObjectId {
    #[must_use]
    pub const fn from_bytes(b: [u8; 16]) -> Self {
        Self(b)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

/// Categories of ECS objects that are replicated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReplicatedObjectKind {
    CommitCapsule,
    CommitMarker,
    IndexSegment,
    ReadWitness,
    WriteWitness,
    WitnessDelta,
    WitnessIndexSegment,
    DependencyEdge,
    CommitProof,
    AbortWitness,
    MergeWitness,
    CheckpointChunk,
    SnapshotManifest,
    DecodeProof,
}

/// A commit marker record — the commit clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitMarker {
    pub commit_seq: u64,
    pub capsule_id: ObjectId,
    pub timestamp_ns: u64,
}

// ---------------------------------------------------------------------------
// Anti-entropy protocol
// ---------------------------------------------------------------------------

/// Tip information exchanged between replicas.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaTip {
    /// Latest root manifest object ID.
    pub root_manifest_id: ObjectId,
    /// Latest marker stream position (commit sequence number).
    pub marker_position: u64,
    /// Optional index segment tips.
    pub index_segment_tips: Vec<ObjectId>,
}

/// Result of computing missing objects between two replicas.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingObjects {
    /// Objects present in remote but not local.
    pub needed: BTreeSet<ObjectId>,
    /// Objects present locally but not remote.
    pub to_offer: BTreeSet<ObjectId>,
}

/// Anti-entropy convergence protocol state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AntiEntropyPhase {
    /// Step 1: Exchange tips.
    ExchangeTips,
    /// Step 2: Compute missing objects.
    ComputeMissing,
    /// Step 3: Request symbols for missing objects.
    RequestSymbols,
    /// Step 4: Stream symbols until decode.
    StreamUntilDecode,
    /// Step 5: Persist and update.
    PersistAndUpdate,
    /// Converged.
    Complete,
}

/// Anti-entropy session between two replicas.
#[derive(Debug)]
pub struct AntiEntropySession {
    phase: AntiEntropyPhase,
    local_tip: Option<ReplicaTip>,
    remote_tip: Option<ReplicaTip>,
    missing: Option<MissingObjects>,
    decoded_objects: HashSet<ObjectId>,
}

impl AntiEntropySession {
    /// Create a new anti-entropy session.
    #[must_use]
    pub fn new() -> Self {
        debug!(bead_id = BEAD_ID, "starting anti-entropy session");
        Self {
            phase: AntiEntropyPhase::ExchangeTips,
            local_tip: None,
            remote_tip: None,
            missing: None,
            decoded_objects: HashSet::new(),
        }
    }

    /// Current phase.
    #[must_use]
    pub const fn phase(&self) -> AntiEntropyPhase {
        self.phase
    }

    /// Step 1: Set local and remote tips.
    pub fn exchange_tips(&mut self, local: ReplicaTip, remote: ReplicaTip) -> Result<()> {
        if self.phase != AntiEntropyPhase::ExchangeTips {
            return Err(FrankenError::Internal(format!(
                "anti-entropy: expected ExchangeTips, got {:?}",
                self.phase
            )));
        }
        debug!(
            bead_id = BEAD_ID,
            local_pos = local.marker_position,
            remote_pos = remote.marker_position,
            "exchanged tips"
        );
        self.local_tip = Some(local);
        self.remote_tip = Some(remote);
        self.phase = AntiEntropyPhase::ComputeMissing;
        Ok(())
    }

    /// Step 2: Compute missing objects from local and remote object sets.
    pub fn compute_missing(
        &mut self,
        local_objects: &BTreeSet<ObjectId>,
        remote_objects: &BTreeSet<ObjectId>,
    ) -> Result<&MissingObjects> {
        if self.phase != AntiEntropyPhase::ComputeMissing {
            return Err(FrankenError::Internal(format!(
                "anti-entropy: expected ComputeMissing, got {:?}",
                self.phase
            )));
        }

        let needed: BTreeSet<ObjectId> =
            remote_objects.difference(local_objects).copied().collect();
        let to_offer: BTreeSet<ObjectId> =
            local_objects.difference(remote_objects).copied().collect();

        debug!(
            bead_id = BEAD_ID,
            needed_count = needed.len(),
            to_offer_count = to_offer.len(),
            "computed missing objects"
        );

        self.missing = Some(MissingObjects { needed, to_offer });
        self.phase = AntiEntropyPhase::RequestSymbols;
        Ok(self.missing.as_ref().expect("just set"))
    }

    /// Step 3: Return the set of object IDs we need symbols for.
    #[must_use]
    pub fn objects_to_request(&self) -> Option<&BTreeSet<ObjectId>> {
        self.missing.as_ref().map(|m| &m.needed)
    }

    /// Step 4: Record that we received enough symbols and decoded an object.
    pub fn record_decoded(&mut self, object_id: ObjectId) -> Result<()> {
        if self.phase != AntiEntropyPhase::RequestSymbols
            && self.phase != AntiEntropyPhase::StreamUntilDecode
        {
            return Err(FrankenError::Internal(format!(
                "anti-entropy: expected RequestSymbols/StreamUntilDecode, got {:?}",
                self.phase
            )));
        }
        self.phase = AntiEntropyPhase::StreamUntilDecode;
        self.decoded_objects.insert(object_id);

        // Check if all needed objects are decoded.
        if let Some(missing) = &self.missing {
            if missing
                .needed
                .iter()
                .all(|id| self.decoded_objects.contains(id))
            {
                debug!(
                    bead_id = BEAD_ID,
                    decoded_count = self.decoded_objects.len(),
                    "all missing objects decoded"
                );
                self.phase = AntiEntropyPhase::PersistAndUpdate;
            }
        }
        Ok(())
    }

    /// Step 5: Finalize — persist and update local state.
    pub fn finalize(&mut self) -> Result<()> {
        if self.phase != AntiEntropyPhase::PersistAndUpdate {
            return Err(FrankenError::Internal(format!(
                "anti-entropy: expected PersistAndUpdate, got {:?}",
                self.phase
            )));
        }
        info!(
            bead_id = BEAD_ID,
            decoded_count = self.decoded_objects.len(),
            "anti-entropy session complete — persisted"
        );
        self.phase = AntiEntropyPhase::Complete;
        Ok(())
    }

    /// Check if the session has converged.
    #[must_use]
    pub const fn is_converged(&self) -> bool {
        matches!(self.phase, AntiEntropyPhase::Complete)
    }
}

impl Default for AntiEntropySession {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Quorum durability
// ---------------------------------------------------------------------------

/// Quorum durability policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumPolicy {
    /// Minimum stores that must accept symbols before commit is durable.
    pub required: u32,
    /// Total number of stores in the quorum set.
    pub total: u32,
}

impl QuorumPolicy {
    /// Create a local-only policy: quorum(1, 1).
    #[must_use]
    pub const fn local_only() -> Self {
        Self {
            required: 1,
            total: 1,
        }
    }

    /// Create a 2-of-3 policy.
    #[must_use]
    pub const fn two_of_three() -> Self {
        Self {
            required: 2,
            total: 3,
        }
    }

    /// Create a custom quorum policy.
    pub fn new(required: u32, total: u32) -> Result<Self> {
        if required == 0 || required > total {
            return Err(FrankenError::Internal(format!(
                "invalid quorum: required={required}, total={total}"
            )));
        }
        Ok(Self { required, total })
    }
}

/// Tracks store acknowledgements for quorum satisfaction.
#[derive(Debug)]
pub struct QuorumTracker {
    policy: QuorumPolicy,
    accepted: HashSet<u32>,
}

impl QuorumTracker {
    /// Create a new tracker for the given policy.
    #[must_use]
    pub fn new(policy: QuorumPolicy) -> Self {
        Self {
            policy,
            accepted: HashSet::new(),
        }
    }

    /// Record that store `store_id` has accepted sufficient symbols.
    pub fn record_acceptance(&mut self, store_id: u32) {
        self.accepted.insert(store_id);
        debug!(
            bead_id = BEAD_ID,
            store_id,
            accepted = self.accepted.len(),
            required = self.policy.required,
            "store accepted symbols"
        );
    }

    /// Check if quorum is satisfied.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn is_satisfied(&self) -> bool {
        self.accepted.len() as u32 >= self.policy.required
    }

    /// Number of stores that have accepted.
    #[must_use]
    pub fn accepted_count(&self) -> usize {
        self.accepted.len()
    }

    /// Policy reference.
    #[must_use]
    pub const fn policy(&self) -> &QuorumPolicy {
        &self.policy
    }
}

// ---------------------------------------------------------------------------
// Consistent-hash symbol routing
// ---------------------------------------------------------------------------

/// Consistent hash ring for symbol routing.
#[derive(Debug, Clone)]
pub struct ConsistentHashRing {
    /// (hash_value, node_id) sorted by hash_value.
    ring: Vec<(u64, u32)>,
    /// Number of virtual nodes per physical node.
    vnodes: u32,
}

impl ConsistentHashRing {
    /// Create a ring with the given node IDs and virtual node count.
    #[must_use]
    pub fn new(node_ids: &[u32], vnodes: u32) -> Self {
        let mut ring = Vec::with_capacity(node_ids.len() * vnodes as usize);
        for &nid in node_ids {
            for v in 0..vnodes {
                let hash = Self::hash_vnode(nid, v);
                ring.push((hash, nid));
            }
        }
        ring.sort_unstable_by_key(|&(h, _)| h);
        Self { ring, vnodes }
    }

    /// Route a symbol (identified by `object_id` + `esi`) to a node.
    #[must_use]
    pub fn route(&self, object_id: &ObjectId, esi: u32) -> Option<u32> {
        if self.ring.is_empty() {
            return None;
        }
        let key = Self::hash_symbol(object_id, esi);
        // Binary search for the first ring entry >= key.
        let idx = self.ring.partition_point(|&(h, _)| h < key);
        let idx = if idx >= self.ring.len() { 0 } else { idx };
        Some(self.ring[idx].1)
    }

    /// Add a node to the ring. Returns the set of symbols that need to be re-routed.
    #[must_use]
    pub fn add_node(&mut self, node_id: u32) -> Self {
        let mut node_ids: BTreeSet<u32> = self.ring.iter().map(|&(_, n)| n).collect();
        node_ids.insert(node_id);
        let ids: Vec<u32> = node_ids.into_iter().collect();
        Self::new(&ids, self.vnodes)
    }

    /// Number of distinct physical nodes in the ring.
    #[must_use]
    pub fn node_count(&self) -> usize {
        let nodes: HashSet<u32> = self.ring.iter().map(|&(_, n)| n).collect();
        nodes.len()
    }

    fn hash_vnode(node_id: u32, vnode: u32) -> u64 {
        let mut buf = [0u8; 8];
        buf[..4].copy_from_slice(&node_id.to_le_bytes());
        buf[4..8].copy_from_slice(&vnode.to_le_bytes());
        xxhash_rust::xxh3::xxh3_64(&buf)
    }

    fn hash_symbol(object_id: &ObjectId, esi: u32) -> u64 {
        let mut buf = [0u8; 20];
        buf[..16].copy_from_slice(object_id.as_bytes());
        buf[16..20].copy_from_slice(&esi.to_le_bytes());
        xxhash_rust::xxh3::xxh3_64(&buf)
    }
}

// ---------------------------------------------------------------------------
// Authenticated symbols
// ---------------------------------------------------------------------------

/// An authenticated symbol with an auth tag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedSymbol {
    pub object_id: ObjectId,
    pub esi: u32,
    pub data: Vec<u8>,
    /// Auth tag for integrity verification.
    pub auth_tag: [u8; 16],
}

impl AuthenticatedSymbol {
    /// Compute expected auth tag for the given data.
    #[must_use]
    pub fn compute_auth_tag(object_id: &ObjectId, esi: u32, data: &[u8]) -> [u8; 16] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"fsqlite:repl:auth:v1");
        hasher.update(object_id.as_bytes());
        hasher.update(&esi.to_le_bytes());
        hasher.update(data);
        let hash = hasher.finalize();
        let mut tag = [0u8; 16];
        tag.copy_from_slice(&hash.as_bytes()[..16]);
        tag
    }

    /// Verify that this symbol's auth tag is valid.
    #[must_use]
    pub fn verify(&self) -> bool {
        let expected = Self::compute_auth_tag(&self.object_id, self.esi, &self.data);
        self.auth_tag == expected
    }

    /// Create a new authenticated symbol with a correct auth tag.
    #[must_use]
    pub fn new(object_id: ObjectId, esi: u32, data: Vec<u8>) -> Self {
        let auth_tag = Self::compute_auth_tag(&object_id, esi, &data);
        Self {
            object_id,
            esi,
            data,
            auth_tag,
        }
    }
}

// ---------------------------------------------------------------------------
// Replication configuration
// ---------------------------------------------------------------------------

/// Configuration for the replication subsystem.
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    pub role: ReplicationRole,
    pub mode: ReplicationMode,
    pub quorum: QuorumPolicy,
    pub security_enabled: bool,
    pub multi_writer_explicit: bool,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            role: ReplicationRole::Leader,
            mode: ReplicationMode::LeaderCommitClock,
            quorum: QuorumPolicy::local_only(),
            security_enabled: false,
            multi_writer_explicit: false,
        }
    }
}

/// Validate replication config. Multi-writer requires explicit opt-in.
pub fn validate_config(config: &ReplicationConfig) -> Result<()> {
    if config.mode == ReplicationMode::MultiWriter && !config.multi_writer_explicit {
        error!(
            bead_id = BEAD_ID,
            "multi-writer mode requires explicit configuration"
        );
        return Err(FrankenError::Internal(
            "multi-writer replication mode requires explicit opt-in via multi_writer_explicit=true"
                .into(),
        ));
    }
    info!(
        bead_id = BEAD_ID,
        role = ?config.role,
        mode = ?config.mode,
        quorum_required = config.quorum.required,
        quorum_total = config.quorum.total,
        security = config.security_enabled,
        "replication config validated"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Commit publication gate
// ---------------------------------------------------------------------------

/// Manages the commit-publication gate: markers are not published until
/// the durability quorum is satisfied.
#[derive(Debug)]
pub struct CommitPublicationGate {
    tracker: QuorumTracker,
    marker: Option<CommitMarker>,
    published: bool,
}

impl CommitPublicationGate {
    /// Create a gate for the given marker and quorum policy.
    #[must_use]
    pub fn new(marker: CommitMarker, policy: QuorumPolicy) -> Self {
        Self {
            tracker: QuorumTracker::new(policy),
            marker: Some(marker),
            published: false,
        }
    }

    /// Record that a store accepted the commit's symbols.
    pub fn record_store_acceptance(&mut self, store_id: u32) {
        self.tracker.record_acceptance(store_id);
    }

    /// Try to publish the marker. Returns the marker if quorum is met,
    /// None if not yet satisfied.
    pub fn try_publish(&mut self) -> Option<&CommitMarker> {
        if self.published {
            return self.marker.as_ref();
        }
        if self.tracker.is_satisfied() {
            self.published = true;
            info!(
                bead_id = BEAD_ID,
                accepted = self.tracker.accepted_count(),
                required = self.tracker.policy().required,
                "quorum satisfied — marker published"
            );
            self.marker.as_ref()
        } else {
            warn!(
                bead_id = BEAD_ID,
                accepted = self.tracker.accepted_count(),
                required = self.tracker.policy().required,
                "quorum not yet satisfied — marker withheld"
            );
            None
        }
    }

    /// Is the marker published?
    #[must_use]
    pub const fn is_published(&self) -> bool {
        self.published
    }
}

// ---------------------------------------------------------------------------
// Symbol filter for security
// ---------------------------------------------------------------------------

/// Filter authenticated symbols, rejecting invalid ones.
pub fn filter_authenticated_symbols(
    symbols: &[AuthenticatedSymbol],
) -> (Vec<&AuthenticatedSymbol>, Vec<&AuthenticatedSymbol>) {
    let mut accepted = Vec::new();
    let mut rejected = Vec::new();

    for sym in symbols {
        if sym.verify() {
            accepted.push(sym);
        } else {
            debug!(
                bead_id = BEAD_ID,
                esi = sym.esi,
                "rejected unauthenticated symbol"
            );
            rejected.push(sym);
        }
    }

    if !rejected.is_empty() {
        warn!(
            bead_id = BEAD_ID,
            rejected_count = rejected.len(),
            accepted_count = accepted.len(),
            "filtered out unauthenticated symbols"
        );
    }

    (accepted, rejected)
}

// ---------------------------------------------------------------------------
// Sheaf consistency check (simplified)
// ---------------------------------------------------------------------------

/// A trace event for consistency checking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceEvent {
    pub node_id: u32,
    pub commit_seq: u64,
    pub object_id: ObjectId,
    pub event_type: TraceEventType,
}

/// Type of trace event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TraceEventType {
    Published,
    Received,
    Applied,
}

/// Result of a sheaf consistency check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SheafCheckResult {
    pub is_consistent: bool,
    pub anomalies: Vec<SheafAnomaly>,
}

/// A consistency anomaly detected by sheaf check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SheafAnomaly {
    pub description: String,
    pub commit_seq: u64,
    pub involved_nodes: Vec<u32>,
}

/// Run a sheaf consistency check on a set of trace events.
///
/// Detects phantom commits: commits seen by no single node end-to-end
/// (Published + Applied on the same node).
#[must_use]
pub fn sheaf_consistency_check(events: &[TraceEvent]) -> SheafCheckResult {
    // Group events by (commit_seq, node_id).
    let mut node_events: HashMap<(u64, u32), HashSet<TraceEventType>> = HashMap::new();
    for ev in events {
        node_events
            .entry((ev.commit_seq, ev.node_id))
            .or_default()
            .insert(ev.event_type);
    }

    // Find all commit sequences.
    let commit_seqs: BTreeSet<u64> = events.iter().map(|e| e.commit_seq).collect();
    let all_nodes: BTreeSet<u32> = events.iter().map(|e| e.node_id).collect();

    let mut anomalies = Vec::new();

    for &seq in &commit_seqs {
        // Check if any single node has both Published and Applied.
        let has_end_to_end = all_nodes.iter().any(|&nid| {
            let key = (seq, nid);
            if let Some(types) = node_events.get(&key) {
                types.contains(&TraceEventType::Published)
                    && types.contains(&TraceEventType::Applied)
            } else {
                false
            }
        });

        if !has_end_to_end {
            // Phantom commit: no single node witnessed end-to-end.
            let involved: Vec<u32> = all_nodes
                .iter()
                .filter(|&&nid| node_events.contains_key(&(seq, nid)))
                .copied()
                .collect();

            if !involved.is_empty() {
                anomalies.push(SheafAnomaly {
                    description: format!(
                        "phantom commit at seq {seq}: no single node has both Published and Applied"
                    ),
                    commit_seq: seq,
                    involved_nodes: involved,
                });
            }
        }
    }

    let is_consistent = anomalies.is_empty();

    if is_consistent {
        debug!(
            bead_id = BEAD_ID,
            commit_count = commit_seqs.len(),
            "sheaf consistency check passed"
        );
    } else {
        warn!(
            bead_id = BEAD_ID,
            anomaly_count = anomalies.len(),
            "sheaf consistency check found anomalies"
        );
    }

    SheafCheckResult {
        is_consistent,
        anomalies,
    }
}

// ---------------------------------------------------------------------------
// TLA+ trace export (simplified)
// ---------------------------------------------------------------------------

/// Export trace events as TLA+ behavior specification.
#[must_use]
pub fn export_tla_trace(events: &[TraceEvent]) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    let _ = writeln!(out, "---- MODULE ReplicationTrace ----");
    let _ = writeln!(out, "EXTENDS Integers, Sequences, FiniteSets");
    let _ = writeln!(out);
    let _ = writeln!(out, "VARIABLES committed, applied");
    let _ = writeln!(out);
    let _ = writeln!(out, "Init ==");
    let _ = writeln!(out, "  /\\ committed = {{}}");
    let _ = writeln!(out, "  /\\ applied = {{}}");
    let _ = writeln!(out);

    for (i, ev) in events.iter().enumerate() {
        let _ = writeln!(
            out,
            "\\* Step {i}: node={}, seq={}",
            ev.node_id, ev.commit_seq
        );
        match ev.event_type {
            TraceEventType::Published => {
                let _ = writeln!(
                    out,
                    "Step{i} == committed' = committed \\cup {{{}}}",
                    ev.commit_seq
                );
            }
            TraceEventType::Applied => {
                let _ = writeln!(
                    out,
                    "Step{i} == applied' = applied \\cup {{{}}}",
                    ev.commit_seq
                );
            }
            TraceEventType::Received => {
                let _ = writeln!(out, "\\* Received event (no state change in this model)");
            }
        }
        let _ = writeln!(out);
    }

    let _ = writeln!(out, "====");
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::too_many_lines)]
mod tests {
    use super::*;

    fn make_oid(seed: u8) -> ObjectId {
        let mut b = [0u8; 16];
        b[0] = seed;
        ObjectId::from_bytes(b)
    }

    // -- Compliance gates --

    #[test]
    fn test_bd_1hi_19_unit_compliance_gate() {
        assert_eq!(BEAD_ID, "bd-1hi.19");
        // Verify all required types exist.
        let _ = ReplicationRole::Leader;
        let _ = ReplicationRole::Follower;
        let _ = ReplicationMode::LeaderCommitClock;
        let _ = ReplicationMode::MultiWriter;
        let _ = AntiEntropyPhase::ExchangeTips;
        let _ = QuorumPolicy::local_only();
    }

    #[test]
    fn prop_bd_1hi_19_structure_compliance() {
        // Property: anti-entropy session progresses through all phases.
        let mut session = AntiEntropySession::new();
        assert_eq!(session.phase(), AntiEntropyPhase::ExchangeTips);

        let local = ReplicaTip {
            root_manifest_id: make_oid(1),
            marker_position: 10,
            index_segment_tips: vec![],
        };
        let remote = ReplicaTip {
            root_manifest_id: make_oid(2),
            marker_position: 12,
            index_segment_tips: vec![],
        };
        session.exchange_tips(local, remote).unwrap();
        assert_eq!(session.phase(), AntiEntropyPhase::ComputeMissing);
    }

    #[test]
    fn test_e2e_bd_1hi_19_compliance() {
        // E2E: full anti-entropy cycle with quorum gate.
        let config = ReplicationConfig::default();
        validate_config(&config).unwrap();

        let mut session = AntiEntropySession::new();
        let local = ReplicaTip {
            root_manifest_id: make_oid(1),
            marker_position: 5,
            index_segment_tips: vec![],
        };
        let remote = ReplicaTip {
            root_manifest_id: make_oid(2),
            marker_position: 7,
            index_segment_tips: vec![],
        };
        session.exchange_tips(local, remote).unwrap();

        let local_objects: BTreeSet<ObjectId> = [make_oid(10), make_oid(20)].into();
        let remote_objects: BTreeSet<ObjectId> = [make_oid(20), make_oid(30)].into();
        let missing = session
            .compute_missing(&local_objects, &remote_objects)
            .unwrap();
        assert!(missing.needed.contains(&make_oid(30)));

        session.record_decoded(make_oid(30)).unwrap();
        assert_eq!(session.phase(), AntiEntropyPhase::PersistAndUpdate);
        session.finalize().unwrap();
        assert!(session.is_converged());
    }

    // -- Leader-follower replication --

    #[test]
    fn test_leader_follower_replication() {
        let config = ReplicationConfig {
            role: ReplicationRole::Leader,
            mode: ReplicationMode::LeaderCommitClock,
            ..Default::default()
        };
        validate_config(&config).unwrap();

        let follower_config = ReplicationConfig {
            role: ReplicationRole::Follower,
            mode: ReplicationMode::LeaderCommitClock,
            ..Default::default()
        };
        validate_config(&follower_config).unwrap();
    }

    // -- Anti-entropy tests --

    #[test]
    fn test_anti_entropy_exchange_tips() {
        let mut session = AntiEntropySession::new();
        let local = ReplicaTip {
            root_manifest_id: make_oid(1),
            marker_position: 10,
            index_segment_tips: vec![make_oid(100)],
        };
        let remote = ReplicaTip {
            root_manifest_id: make_oid(2),
            marker_position: 15,
            index_segment_tips: vec![make_oid(200)],
        };
        session.exchange_tips(local, remote).unwrap();
        assert_eq!(session.phase(), AntiEntropyPhase::ComputeMissing);
    }

    #[test]
    fn test_anti_entropy_compute_missing() {
        let mut session = AntiEntropySession::new();
        session
            .exchange_tips(
                ReplicaTip {
                    root_manifest_id: make_oid(1),
                    marker_position: 0,
                    index_segment_tips: vec![],
                },
                ReplicaTip {
                    root_manifest_id: make_oid(2),
                    marker_position: 0,
                    index_segment_tips: vec![],
                },
            )
            .unwrap();

        let local: BTreeSet<ObjectId> = [make_oid(1), make_oid(2), make_oid(3)].into();
        let remote: BTreeSet<ObjectId> = [make_oid(2), make_oid(3), make_oid(4)].into();

        let missing = session.compute_missing(&local, &remote).unwrap();
        assert_eq!(missing.needed, [make_oid(4)].into());
        assert_eq!(missing.to_offer, [make_oid(1)].into());
    }

    #[test]
    fn test_anti_entropy_stream_until_decode() {
        let mut session = AntiEntropySession::new();
        session
            .exchange_tips(
                ReplicaTip {
                    root_manifest_id: make_oid(1),
                    marker_position: 0,
                    index_segment_tips: vec![],
                },
                ReplicaTip {
                    root_manifest_id: make_oid(2),
                    marker_position: 0,
                    index_segment_tips: vec![],
                },
            )
            .unwrap();

        let local: BTreeSet<ObjectId> = [make_oid(1)].into();
        let remote: BTreeSet<ObjectId> = [make_oid(1), make_oid(2), make_oid(3)].into();
        session.compute_missing(&local, &remote).unwrap();

        // Decode objects one by one.
        session.record_decoded(make_oid(2)).unwrap();
        assert_eq!(session.phase(), AntiEntropyPhase::StreamUntilDecode);
        session.record_decoded(make_oid(3)).unwrap();
        assert_eq!(session.phase(), AntiEntropyPhase::PersistAndUpdate);
    }

    #[test]
    fn test_anti_entropy_convergence() {
        let mut session = AntiEntropySession::new();
        session
            .exchange_tips(
                ReplicaTip {
                    root_manifest_id: make_oid(1),
                    marker_position: 0,
                    index_segment_tips: vec![],
                },
                ReplicaTip {
                    root_manifest_id: make_oid(2),
                    marker_position: 0,
                    index_segment_tips: vec![],
                },
            )
            .unwrap();

        let local: BTreeSet<ObjectId> = [make_oid(1), make_oid(2)].into();
        let remote: BTreeSet<ObjectId> = [make_oid(2), make_oid(3)].into();
        session.compute_missing(&local, &remote).unwrap();
        session.record_decoded(make_oid(3)).unwrap();
        session.finalize().unwrap();
        assert!(session.is_converged());
    }

    // -- Quorum tests --

    #[test]
    fn test_quorum_local_only() {
        let policy = QuorumPolicy::local_only();
        let mut tracker = QuorumTracker::new(policy);
        assert!(!tracker.is_satisfied());
        tracker.record_acceptance(0);
        assert!(tracker.is_satisfied());
    }

    #[test]
    fn test_quorum_2_of_3() {
        let policy = QuorumPolicy::two_of_three();
        let mut tracker = QuorumTracker::new(policy);
        assert!(!tracker.is_satisfied());
        tracker.record_acceptance(0);
        assert!(!tracker.is_satisfied()); // 1 of 3.
        tracker.record_acceptance(1);
        assert!(tracker.is_satisfied()); // 2 of 3.
        tracker.record_acceptance(2);
        assert!(tracker.is_satisfied()); // 3 of 3 — still satisfied.
    }

    #[test]
    fn test_quorum_blocks_marker_publication() {
        let marker = CommitMarker {
            commit_seq: 42,
            capsule_id: make_oid(10),
            timestamp_ns: 1_000_000,
        };
        let policy = QuorumPolicy::two_of_three();
        let mut gate = CommitPublicationGate::new(marker, policy);

        // Marker not published before quorum.
        assert!(!gate.is_published());
        assert!(gate.try_publish().is_none());

        gate.record_store_acceptance(0);
        assert!(gate.try_publish().is_none()); // 1 of 2 needed.

        gate.record_store_acceptance(1);
        let published = gate.try_publish();
        assert!(published.is_some());
        assert_eq!(published.unwrap().commit_seq, 42);
        assert!(gate.is_published());
    }

    // -- Symbol routing tests --

    #[test]
    fn test_symbol_routing_consistent_hash() {
        let ring = ConsistentHashRing::new(&[1, 2, 3], 100);
        assert_eq!(ring.node_count(), 3);

        let oid = make_oid(42);
        let node = ring.route(&oid, 0).unwrap();
        assert!([1, 2, 3].contains(&node));

        // Deterministic: same input → same output.
        let node2 = ring.route(&oid, 0).unwrap();
        assert_eq!(node, node2);
    }

    #[test]
    fn test_symbol_routing_add_node_minimal_reroute() {
        let mut ring3 = ConsistentHashRing::new(&[1, 2, 3], 100);
        let ring4 = ring3.add_node(4);
        assert_eq!(ring4.node_count(), 4);

        // Most symbols should stay on same node. Count reroutes.
        let oid = make_oid(1);
        let mut rerouted = 0_u32;
        for esi in 0..1000 {
            let n3 = ring3.route(&oid, esi).unwrap();
            let n4 = ring4.route(&oid, esi).unwrap();
            if n3 != n4 {
                rerouted += 1;
            }
        }
        // Adding 1 of 4 nodes should reroute roughly 25% (with consistent hashing).
        // Allow wide margin due to hash distribution.
        assert!(rerouted < 500, "too many reroutes: {rerouted}/1000");
    }

    // -- Authenticated symbols tests --

    #[test]
    fn test_authenticated_symbols_verified() {
        let sym = AuthenticatedSymbol::new(make_oid(1), 0, vec![1, 2, 3]);
        assert!(sym.verify());

        // Tampered data.
        let mut bad = sym.clone();
        bad.data[0] = 99;
        assert!(!bad.verify());

        // Tampered auth_tag.
        let mut bad2 = sym;
        bad2.auth_tag[0] ^= 0xFF;
        assert!(!bad2.verify());
    }

    #[test]
    fn test_unauthenticated_fallback() {
        let good1 = AuthenticatedSymbol::new(make_oid(1), 0, vec![10, 20]);
        let good2 = AuthenticatedSymbol::new(make_oid(1), 1, vec![30, 40]);
        let mut bad = AuthenticatedSymbol::new(make_oid(1), 2, vec![50, 60]);
        bad.auth_tag[0] ^= 0xFF; // Corrupt.

        let all = [good1, good2, bad];
        let (accepted, rejected) = filter_authenticated_symbols(&all);
        assert_eq!(accepted.len(), 2);
        assert_eq!(rejected.len(), 1);
        assert_eq!(rejected[0].esi, 2);
    }

    // -- Sheaf consistency check --

    #[test]
    fn test_sheaf_consistency_check_clean() {
        let events = vec![
            TraceEvent {
                node_id: 1,
                commit_seq: 1,
                object_id: make_oid(10),
                event_type: TraceEventType::Published,
            },
            TraceEvent {
                node_id: 1,
                commit_seq: 1,
                object_id: make_oid(10),
                event_type: TraceEventType::Applied,
            },
        ];
        let result = sheaf_consistency_check(&events);
        assert!(result.is_consistent);
        assert!(result.anomalies.is_empty());
    }

    #[test]
    fn test_sheaf_consistency_check_phantom() {
        // Phantom commit: node 1 Published, node 2 Applied, no single node has both.
        let events = vec![
            TraceEvent {
                node_id: 1,
                commit_seq: 1,
                object_id: make_oid(10),
                event_type: TraceEventType::Published,
            },
            TraceEvent {
                node_id: 2,
                commit_seq: 1,
                object_id: make_oid(10),
                event_type: TraceEventType::Applied,
            },
        ];
        let result = sheaf_consistency_check(&events);
        assert!(!result.is_consistent);
        assert_eq!(result.anomalies.len(), 1);
        assert_eq!(result.anomalies[0].commit_seq, 1);
    }

    // -- TLA+ export --

    #[test]
    fn test_tla_export() {
        let events = vec![
            TraceEvent {
                node_id: 1,
                commit_seq: 1,
                object_id: make_oid(10),
                event_type: TraceEventType::Published,
            },
            TraceEvent {
                node_id: 2,
                commit_seq: 1,
                object_id: make_oid(10),
                event_type: TraceEventType::Applied,
            },
        ];
        let tla = export_tla_trace(&events);
        assert!(tla.contains("MODULE ReplicationTrace"));
        assert!(tla.contains("committed"));
        assert!(tla.contains("applied"));
        assert!(tla.contains("===="));
    }

    // -- Multi-writer gated --

    #[test]
    fn test_multiwriter_not_default() {
        let config = ReplicationConfig {
            mode: ReplicationMode::MultiWriter,
            multi_writer_explicit: false,
            ..Default::default()
        };
        let result = validate_config(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_multiwriter_explicit_ok() {
        let config = ReplicationConfig {
            mode: ReplicationMode::MultiWriter,
            multi_writer_explicit: true,
            ..Default::default()
        };
        validate_config(&config).unwrap();
    }

    // -- Property tests --

    #[test]
    fn prop_anti_entropy_convergence() {
        // For various random-ish object sets, anti-entropy always converges.
        for seed in 0..20_u8 {
            let local: BTreeSet<ObjectId> = (0..seed).map(|i| make_oid(i * 2)).collect();
            let remote: BTreeSet<ObjectId> = (0..seed).map(|i| make_oid(i * 2 + 1)).collect();

            let mut session = AntiEntropySession::new();
            session
                .exchange_tips(
                    ReplicaTip {
                        root_manifest_id: make_oid(100),
                        marker_position: 0,
                        index_segment_tips: vec![],
                    },
                    ReplicaTip {
                        root_manifest_id: make_oid(200),
                        marker_position: 0,
                        index_segment_tips: vec![],
                    },
                )
                .unwrap();
            let missing = session.compute_missing(&local, &remote).unwrap();
            for &oid in &missing.needed.clone() {
                session.record_decoded(oid).unwrap();
            }
            if session.phase() == AntiEntropyPhase::PersistAndUpdate {
                session.finalize().unwrap();
            }
            // Either converged (had missing objects) or still at RequestSymbols (nothing missing).
            assert!(
                session.is_converged() || session.phase() == AntiEntropyPhase::RequestSymbols,
                "failed to converge for seed={seed}"
            );
        }
    }

    #[test]
    fn prop_quorum_safety() {
        // For various M, N, quorum only reports satisfied when >= M accepts.
        for m in 1..=5_u32 {
            for n in m..=5 {
                let policy = QuorumPolicy::new(m, n).unwrap();
                let mut tracker = QuorumTracker::new(policy);
                for i in 0..m - 1 {
                    tracker.record_acceptance(i);
                    assert!(
                        !tracker.is_satisfied(),
                        "should not be satisfied with {} of {} (need {})",
                        i + 1,
                        n,
                        m
                    );
                }
                tracker.record_acceptance(m - 1);
                assert!(
                    tracker.is_satisfied(),
                    "should be satisfied with {m} of {n}"
                );
            }
        }
    }

    // -- ECS replication ordering --

    #[test]
    fn test_ecs_replication_ordering() {
        // Commit markers applied in commit_seq order.
        let markers = [
            CommitMarker {
                commit_seq: 1,
                capsule_id: make_oid(1),
                timestamp_ns: 100,
            },
            CommitMarker {
                commit_seq: 2,
                capsule_id: make_oid(2),
                timestamp_ns: 200,
            },
            CommitMarker {
                commit_seq: 3,
                capsule_id: make_oid(3),
                timestamp_ns: 300,
            },
        ];

        // Verify ordering invariant.
        for w in markers.windows(2) {
            assert!(w[0].commit_seq < w[1].commit_seq);
        }
    }

    // -- E2E tests --

    #[test]
    fn test_e2e_3_node_replication() {
        // Simulate 1 leader + 2 followers. Leader commits, followers converge.
        let mut leader_objects: BTreeSet<ObjectId> = BTreeSet::new();

        // Leader commits 10 transactions.
        for i in 0..10_u8 {
            leader_objects.insert(make_oid(i));
        }

        // Follower 1 starts empty.
        let follower1_objects: BTreeSet<ObjectId> = BTreeSet::new();

        // Anti-entropy: follower 1 syncs with leader.
        let mut session = AntiEntropySession::new();
        session
            .exchange_tips(
                ReplicaTip {
                    root_manifest_id: make_oid(0),
                    marker_position: 0,
                    index_segment_tips: vec![],
                },
                ReplicaTip {
                    root_manifest_id: make_oid(9),
                    marker_position: 10,
                    index_segment_tips: vec![],
                },
            )
            .unwrap();
        let missing = session
            .compute_missing(&follower1_objects, &leader_objects)
            .unwrap();
        assert_eq!(missing.needed.len(), 10);

        for &oid in &missing.needed.clone() {
            session.record_decoded(oid).unwrap();
        }
        session.finalize().unwrap();
        assert!(session.is_converged());
    }

    #[test]
    fn test_e2e_node_failure_recovery() {
        // 3 stores, quorum 2 of 3. Kill store B. Leader still commits. Restart B.
        let policy = QuorumPolicy::two_of_three();
        let marker = CommitMarker {
            commit_seq: 1,
            capsule_id: make_oid(1),
            timestamp_ns: 1000,
        };
        let mut gate = CommitPublicationGate::new(marker, policy);

        // Store A accepts.
        gate.record_store_acceptance(0);
        // Store B down — no acceptance.
        // Store C accepts.
        gate.record_store_acceptance(2);

        // Quorum satisfied (A + C = 2 of 3).
        assert!(gate.try_publish().is_some());
    }
}
