//! §3.5.7 RaptorQ permeation map audit (`bd-1hi.27`).
//!
//! This module enforces the rule that every subsystem that persists or ships
//! bytes declares an ECS object type, symbol policy, and repair story.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::fmt;

use fsqlite_types::ObjectId;
use tracing::{debug, error, info, warn};

/// Bead identifier for this module.
pub const PERMEATION_BEAD_ID: &str = "bd-1hi.27";
/// Structured logging reference for this module.
pub const PERMEATION_LOGGING_STANDARD: &str = "bd-1fpm";

const IBLT_HASH_COUNT: usize = 3;
const IBLT_HASH_SEEDS: [u64; IBLT_HASH_COUNT] = [
    0x9E37_79B9_7F4A_7C15,
    0xC2B2_AE3D_27D4_EB4F,
    0x1656_67B1_9E37_79F9,
];

/// ECS permeation plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Plane {
    /// Durable storage plane.
    Durability,
    /// In-memory concurrency/visibility plane.
    Concurrency,
    /// Replication/transport plane.
    Replication,
    /// Explainability/observability plane.
    Observability,
}

impl fmt::Display for Plane {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Durability => f.write_str("Durability"),
            Self::Concurrency => f.write_str("Concurrency"),
            Self::Replication => f.write_str("Replication"),
            Self::Observability => f.write_str("Observability"),
        }
    }
}

/// One permeation-map declaration entry (§3.5.7 checklist).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PermeationEntry {
    /// Subsystem name.
    pub subsystem: &'static str,
    /// ECS object type or transport primitive.
    pub object_type: &'static str,
    /// Symbol size/redundancy policy declaration.
    pub symbol_size_policy: &'static str,
    /// Repair story declaration.
    pub repair_story: &'static str,
    /// Architecture plane.
    pub plane: Plane,
}

/// V1 required subsystem names.
pub static V1_REQUIRED_SUBSYSTEMS: &[&str] = &[
    "Commits/CapsuleProof",
    "Commits/MarkerStream",
    "Checkpoints",
    "Indices",
    "Page storage",
    "MVCC page history",
    "Conflict reduction",
    "SSI witness plane",
    "Symbol streaming",
    "Anti-entropy",
    "Bootstrap",
    "Multipath",
    "Repair auditing",
    "Schedule exploration",
    "Invariant monitoring",
    "Model checking",
];

/// Canonical permeation map for V1.
pub static PERMEATION_MAP: &[PermeationEntry] = &[
    PermeationEntry {
        subsystem: "Commits/CapsuleProof",
        object_type: "CommitCapsule+CommitProof",
        symbol_size_policy: "T=min(page_size,4096), R=20%",
        repair_story: "decode from surviving symbols",
        plane: Plane::Durability,
    },
    PermeationEntry {
        subsystem: "Commits/MarkerStream",
        object_type: "CommitMarkerRecord",
        symbol_size_policy: "fixed:88B record stream (no fountain)",
        repair_story: "torn-tail ignore + record_xxh3 + hash-chain audit",
        plane: Plane::Durability,
    },
    PermeationEntry {
        subsystem: "Checkpoints",
        object_type: "CheckpointChunk",
        symbol_size_policy: "T=1024-4096B, R=policy-driven",
        repair_story: "chunked snapshot objects; rebuild from marker stream if lost",
        plane: Plane::Durability,
    },
    PermeationEntry {
        subsystem: "Indices",
        object_type: "IndexSegment",
        symbol_size_policy: "T=1280-4096B, R=20%",
        repair_story: "decode or rebuild-from-marker-scan",
        plane: Plane::Durability,
    },
    PermeationEntry {
        subsystem: "Page storage",
        object_type: "PageHistory",
        symbol_size_policy: "T=page_size, R=per-group",
        repair_story: "decode from group symbols; on-the-fly repair on read",
        plane: Plane::Durability,
    },
    PermeationEntry {
        subsystem: "MVCC page history",
        object_type: "PageHistoryPatchChain",
        symbol_size_policy: "T=page_size, R=per-group",
        repair_story: "bounded by GC horizon; repair through patch replay",
        plane: Plane::Concurrency,
    },
    PermeationEntry {
        subsystem: "Conflict reduction",
        object_type: "IntentLog",
        symbol_size_policy: "T=256-1024B, R=policy-driven",
        repair_story: "replayed deterministically for rebase merge",
        plane: Plane::Concurrency,
    },
    PermeationEntry {
        subsystem: "SSI witness plane",
        object_type: "ReadWitness+WriteWitness+WitnessIndexSegment+DependencyEdge+CommitProof",
        symbol_size_policy: "T=1280-4096B, R=policy-driven",
        repair_story: "decode witness stream and rebuild serialization graph",
        plane: Plane::Concurrency,
    },
    PermeationEntry {
        subsystem: "Symbol streaming",
        object_type: "SymbolSink/SymbolStream",
        symbol_size_policy: "T=1280-4096B, R=transport-policy",
        repair_story: "symbol-native transport; recover with any K symbols",
        plane: Plane::Replication,
    },
    PermeationEntry {
        subsystem: "Anti-entropy",
        object_type: "ObjectIdSetIBLT",
        symbol_size_policy: "fixed:16B object-id atoms (IBLT), R=0%",
        repair_story: "peel IBLT; fallback to segment hash scan on overflow",
        plane: Plane::Replication,
    },
    PermeationEntry {
        subsystem: "Bootstrap",
        object_type: "CheckpointChunk",
        symbol_size_policy: "T=1024-4096B, R=policy-driven",
        repair_story: "late join by collecting K checkpoint symbols",
        plane: Plane::Replication,
    },
    PermeationEntry {
        subsystem: "Multipath",
        object_type: "MultipathAggregator",
        symbol_size_policy: "T=1280-4096B, R=transport-policy",
        repair_story: "any K symbols from any path reconstructs object",
        plane: Plane::Replication,
    },
    PermeationEntry {
        subsystem: "Repair auditing",
        object_type: "DecodeProof",
        symbol_size_policy: "T=1024-4096B, R=0%",
        repair_story: "attach decode proof artifacts to deterministic traces",
        plane: Plane::Observability,
    },
    PermeationEntry {
        subsystem: "Schedule exploration",
        object_type: "LabRuntimeTrace",
        symbol_size_policy: "T=1024-4096B, R=0%",
        repair_story: "deterministic replay from seed and event stream",
        plane: Plane::Observability,
    },
    PermeationEntry {
        subsystem: "Invariant monitoring",
        object_type: "EProcessMonitorEvent",
        symbol_size_policy: "T=256-1024B, R=0%",
        repair_story: "stream invariant events and enforce corruption budgets",
        plane: Plane::Observability,
    },
    PermeationEntry {
        subsystem: "Model checking",
        object_type: "TlaExportTrace",
        symbol_size_policy: "T=1024-4096B, R=0%",
        repair_story: "export traces for bounded TLA+ model checking",
        plane: Plane::Observability,
    },
];

/// Parser output for a symbol policy declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParsedSymbolPolicy {
    /// Symbol size policy.
    pub symbol_size: SymbolSizePolicy,
    /// Redundancy policy.
    pub redundancy: RedundancyPolicy,
    /// Whether this policy is fountain-coded.
    pub fountain_coded: bool,
}

/// Symbol-size policy grammar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SymbolSizePolicy {
    /// `T=min(page_size,<cap>)`
    MinPageSize { cap_bytes: u32 },
    /// `T=page_size`
    PageSize,
    /// `T=<lo>-<hi>B`
    RangeBytes { min_bytes: u32, max_bytes: u32 },
    /// `T=<fixed>B`
    FixedBytes(u32),
}

/// Redundancy policy grammar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedundancyPolicy {
    /// Explicit percent, represented as basis points.
    PercentBps(u16),
    /// Policy-picked redundancy.
    PolicyDriven,
    /// Per-group redundancy.
    PerGroup,
    /// Transport policy redundancy.
    TransportPolicy,
}

/// Concrete symbol policy resolved for a page size + defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedSymbolPolicy {
    /// Concrete symbol size in bytes.
    pub symbol_size_bytes: u32,
    /// Concrete redundancy in basis points.
    pub redundancy_bps: u16,
    /// Whether this remains fountain-coded after resolution.
    pub fountain_coded: bool,
}

/// Resolution defaults for non-numeric redundancy declarations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PolicyResolutionDefaults {
    /// Default for `policy-driven`.
    pub policy_driven_bps: u16,
    /// Default for `per-group`.
    pub per_group_bps: u16,
    /// Default for `transport-policy`.
    pub transport_policy_bps: u16,
}

impl Default for PolicyResolutionDefaults {
    fn default() -> Self {
        Self {
            policy_driven_bps: 2_000,
            per_group_bps: 2_000,
            transport_policy_bps: 1_500,
        }
    }
}

impl ParsedSymbolPolicy {
    /// Resolve a parse policy into concrete values.
    #[must_use]
    pub fn resolve(
        self,
        page_size: u32,
        defaults: PolicyResolutionDefaults,
    ) -> ResolvedSymbolPolicy {
        let symbol_size_bytes = match self.symbol_size {
            SymbolSizePolicy::MinPageSize { cap_bytes } => page_size.min(cap_bytes),
            SymbolSizePolicy::PageSize => page_size,
            SymbolSizePolicy::RangeBytes {
                min_bytes,
                max_bytes,
            } => page_size.clamp(min_bytes, max_bytes),
            SymbolSizePolicy::FixedBytes(bytes) => bytes,
        };

        let redundancy_bps = match self.redundancy {
            RedundancyPolicy::PercentBps(bps) => bps,
            RedundancyPolicy::PolicyDriven => defaults.policy_driven_bps,
            RedundancyPolicy::PerGroup => defaults.per_group_bps,
            RedundancyPolicy::TransportPolicy => defaults.transport_policy_bps,
        };

        ResolvedSymbolPolicy {
            symbol_size_bytes,
            redundancy_bps,
            fountain_coded: self.fountain_coded,
        }
    }
}

/// Parse error for symbol policy declarations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SymbolPolicyParseError {
    detail: String,
}

impl SymbolPolicyParseError {
    fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }
}

impl fmt::Display for SymbolPolicyParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.detail)
    }
}

impl Error for SymbolPolicyParseError {}

/// Audit failure category.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditFailureKind {
    /// Entry missing for a required subsystem.
    MissingEntry,
    /// Duplicate subsystem declaration in the same plane.
    DuplicateSubsystemInPlane,
    /// Empty declaration field.
    EmptyField,
    /// Unparseable symbol policy.
    InvalidSymbolPolicy,
}

/// One audit failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditFailure {
    /// Failure category.
    pub kind: AuditFailureKind,
    /// Subsystem name associated with this failure.
    pub subsystem: String,
    /// Plane, when available.
    pub plane: Option<Plane>,
    /// Human-readable detail.
    pub detail: String,
}

/// Parse one symbol policy declaration.
///
/// # Errors
///
/// Returns [`SymbolPolicyParseError`] when the string does not match the
/// expected grammar.
pub fn parse_symbol_policy(raw: &str) -> Result<ParsedSymbolPolicy, SymbolPolicyParseError> {
    if let Some((bytes, redundancy)) = parse_fixed_policy(raw) {
        let redundancy = parse_redundancy_policy(redundancy)?;
        return Ok(ParsedSymbolPolicy {
            symbol_size: SymbolSizePolicy::FixedBytes(bytes),
            redundancy,
            fountain_coded: false,
        });
    }

    let (symbol_raw, redundancy_raw) = raw.split_once(", R=").ok_or_else(|| {
        SymbolPolicyParseError::new(format!("policy missing ', R=' clause: {raw}"))
    })?;

    let symbol_size = parse_symbol_size_policy(symbol_raw.trim())?;
    let redundancy = parse_redundancy_policy(redundancy_raw.trim())?;
    Ok(ParsedSymbolPolicy {
        symbol_size,
        redundancy,
        fountain_coded: true,
    })
}

/// Run the permeation-map audit on the canonical V1 map.
#[must_use]
pub fn audit_permeation_map() -> Vec<AuditFailure> {
    audit_permeation_entries(
        PERMEATION_MAP,
        V1_REQUIRED_SUBSYSTEMS,
        4096,
        PolicyResolutionDefaults::default(),
    )
}

/// Run the permeation-map audit against arbitrary entries.
///
/// This helper is used by tests to enforce "new subsystem requires entry".
#[must_use]
pub fn audit_permeation_entries(
    entries: &[PermeationEntry],
    required_subsystems: &[&str],
    page_size: u32,
    defaults: PolicyResolutionDefaults,
) -> Vec<AuditFailure> {
    debug!(
        bead_id = PERMEATION_BEAD_ID,
        logging_standard = PERMEATION_LOGGING_STANDARD,
        entry_count = entries.len(),
        required_count = required_subsystems.len(),
        page_size = page_size,
        "starting permeation-map audit"
    );

    let mut failures = Vec::new();
    let mut seen = BTreeSet::new();
    let mut by_subsystem: BTreeMap<&str, usize> = BTreeMap::new();

    for entry in entries {
        *by_subsystem.entry(entry.subsystem).or_default() += 1;
        push_empty_field_failures(&mut failures, entry);

        if !seen.insert((entry.plane, entry.subsystem)) {
            failures.push(AuditFailure {
                kind: AuditFailureKind::DuplicateSubsystemInPlane,
                subsystem: entry.subsystem.to_owned(),
                plane: Some(entry.plane),
                detail: format!(
                    "duplicate subsystem '{}' in plane {}",
                    entry.subsystem, entry.plane
                ),
            });
        }

        validate_symbol_policy_entry(&mut failures, entry, page_size, defaults);
    }

    for required in required_subsystems {
        if !by_subsystem.contains_key(required) {
            failures.push(AuditFailure {
                kind: AuditFailureKind::MissingEntry,
                subsystem: (*required).to_owned(),
                plane: None,
                detail: "required subsystem missing from permeation map".to_owned(),
            });
        }
    }

    if failures.is_empty() {
        info!(
            bead_id = PERMEATION_BEAD_ID,
            logging_standard = PERMEATION_LOGGING_STANDARD,
            entry_count = entries.len(),
            "permeation-map audit complete: no gaps"
        );
    } else {
        error!(
            bead_id = PERMEATION_BEAD_ID,
            logging_standard = PERMEATION_LOGGING_STANDARD,
            entry_count = entries.len(),
            failure_count = failures.len(),
            "permeation-map audit detected failures"
        );
    }

    failures
}

fn push_empty_field_failures(failures: &mut Vec<AuditFailure>, entry: &PermeationEntry) {
    if entry.subsystem.trim().is_empty() {
        failures.push(AuditFailure {
            kind: AuditFailureKind::EmptyField,
            subsystem: entry.subsystem.to_owned(),
            plane: Some(entry.plane),
            detail: "subsystem is empty".to_owned(),
        });
    }
    if entry.object_type.trim().is_empty() {
        failures.push(AuditFailure {
            kind: AuditFailureKind::EmptyField,
            subsystem: entry.subsystem.to_owned(),
            plane: Some(entry.plane),
            detail: "object_type is empty".to_owned(),
        });
    }
    if entry.symbol_size_policy.trim().is_empty() {
        failures.push(AuditFailure {
            kind: AuditFailureKind::EmptyField,
            subsystem: entry.subsystem.to_owned(),
            plane: Some(entry.plane),
            detail: "symbol_size_policy is empty".to_owned(),
        });
    }
    if entry.repair_story.trim().is_empty() {
        failures.push(AuditFailure {
            kind: AuditFailureKind::EmptyField,
            subsystem: entry.subsystem.to_owned(),
            plane: Some(entry.plane),
            detail: "repair_story is empty".to_owned(),
        });
    }
}

fn validate_symbol_policy_entry(
    failures: &mut Vec<AuditFailure>,
    entry: &PermeationEntry,
    page_size: u32,
    defaults: PolicyResolutionDefaults,
) {
    match parse_symbol_policy(entry.symbol_size_policy) {
        Ok(parsed) => {
            let resolved = parsed.resolve(page_size, defaults);
            debug!(
                bead_id = PERMEATION_BEAD_ID,
                logging_standard = PERMEATION_LOGGING_STANDARD,
                subsystem = entry.subsystem,
                plane = %entry.plane,
                symbol_size_bytes = resolved.symbol_size_bytes,
                redundancy_bps = resolved.redundancy_bps,
                fountain_coded = resolved.fountain_coded,
                "validated symbol policy declaration"
            );
        }
        Err(parse_error) => {
            error!(
                bead_id = PERMEATION_BEAD_ID,
                logging_standard = PERMEATION_LOGGING_STANDARD,
                subsystem = entry.subsystem,
                plane = %entry.plane,
                policy = entry.symbol_size_policy,
                error = %parse_error,
                "invalid permeation symbol policy"
            );
            failures.push(AuditFailure {
                kind: AuditFailureKind::InvalidSymbolPolicy,
                subsystem: entry.subsystem.to_owned(),
                plane: Some(entry.plane),
                detail: parse_error.to_string(),
            });
        }
    }
}

/// Reconciliation delta between two object-id sets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationDelta {
    /// Object IDs present in remote but missing locally.
    pub missing_locally: BTreeSet<ObjectId>,
    /// Object IDs present in local but missing remotely.
    pub missing_remotely: BTreeSet<ObjectId>,
}

impl ReconciliationDelta {
    /// Whether both sides are converged.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.missing_locally.is_empty() && self.missing_remotely.is_empty()
    }
}

/// Reconciliation result, including fallback flag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationResult {
    /// Symmetric-difference delta.
    pub delta: ReconciliationDelta,
    /// True when segment-hash fallback was used.
    pub used_fallback: bool,
}

/// Errors emitted by IBLT operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IbltError {
    /// Invalid number of cells.
    InvalidCellCount { cell_count: usize },
    /// Shape mismatch between two IBLTs.
    ShapeMismatch {
        left_cell_count: usize,
        right_cell_count: usize,
    },
    /// Peeling failed due to overflow/collision pressure.
    PeelOverflow { residual_cells: usize },
}

impl fmt::Display for IbltError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCellCount { cell_count } => {
                write!(f, "invalid IBLT cell count: {cell_count}")
            }
            Self::ShapeMismatch {
                left_cell_count,
                right_cell_count,
            } => write!(
                f,
                "IBLT shape mismatch: left={left_cell_count}, right={right_cell_count}"
            ),
            Self::PeelOverflow { residual_cells } => {
                write!(f, "IBLT peel failed with {residual_cells} residual cells")
            }
        }
    }
}

impl Error for IbltError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct IbltCell {
    count: i32,
    key_xor: [u8; 16],
    checksum_xor: u32,
}

impl IbltCell {
    fn is_zero(self) -> bool {
        self.count == 0 && self.key_xor == [0_u8; 16] && self.checksum_xor == 0
    }

    fn is_pure(self) -> bool {
        if self.count.unsigned_abs() != 1 {
            return false;
        }
        checksum_for_bytes(&self.key_xor) == self.checksum_xor
    }
}

/// Simple ObjectId IBLT implementation for anti-entropy reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectIdIblt {
    cells: Vec<IbltCell>,
}

impl ObjectIdIblt {
    /// Construct an empty IBLT.
    ///
    /// # Errors
    ///
    /// Returns [`IbltError::InvalidCellCount`] for unusable cell counts.
    pub fn new(cell_count: usize) -> Result<Self, IbltError> {
        if cell_count < IBLT_HASH_COUNT {
            return Err(IbltError::InvalidCellCount { cell_count });
        }
        Ok(Self {
            cells: vec![IbltCell::default(); cell_count],
        })
    }

    /// Build an IBLT from a set of object IDs.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::new`].
    pub fn from_set(object_ids: &BTreeSet<ObjectId>, cell_count: usize) -> Result<Self, IbltError> {
        let mut iblt = Self::new(cell_count)?;
        for object_id in object_ids {
            iblt.insert(*object_id);
        }
        Ok(iblt)
    }

    fn insert(&mut self, object_id: ObjectId) {
        self.apply_delta(object_id, 1);
    }

    fn apply_delta(&mut self, object_id: ObjectId, delta: i32) {
        let checksum = checksum_for_bytes(object_id.as_bytes());
        for index in bucket_indices(object_id, self.cells.len()) {
            let cell = &mut self.cells[index];
            cell.count += delta;
            xor_in_place(&mut cell.key_xor, object_id.as_bytes());
            cell.checksum_xor ^= checksum;
        }
    }

    fn subtract_assign(&mut self, rhs: &Self) -> Result<(), IbltError> {
        if self.cells.len() != rhs.cells.len() {
            return Err(IbltError::ShapeMismatch {
                left_cell_count: self.cells.len(),
                right_cell_count: rhs.cells.len(),
            });
        }

        for (left, right) in self.cells.iter_mut().zip(rhs.cells.iter()) {
            left.count -= right.count;
            xor_in_place(&mut left.key_xor, &right.key_xor);
            left.checksum_xor ^= right.checksum_xor;
        }
        Ok(())
    }

    fn peel(self) -> Result<ReconciliationDelta, IbltError> {
        let mut working = self;
        let mut queue = VecDeque::new();
        for (index, cell) in working.cells.iter().enumerate() {
            if cell.is_pure() {
                queue.push_back(index);
            }
        }

        let mut missing_locally = BTreeSet::new();
        let mut missing_remotely = BTreeSet::new();

        while let Some(index) = queue.pop_front() {
            let cell = working.cells[index];
            if !cell.is_pure() {
                continue;
            }

            let sign = cell.count.signum();
            if sign == 0 {
                continue;
            }

            let object_id = ObjectId::from_bytes(cell.key_xor);
            if sign > 0 {
                missing_locally.insert(object_id);
            } else {
                missing_remotely.insert(object_id);
            }

            let checksum = checksum_for_bytes(object_id.as_bytes());
            for bucket in bucket_indices(object_id, working.cells.len()) {
                let target = &mut working.cells[bucket];
                target.count -= sign;
                xor_in_place(&mut target.key_xor, object_id.as_bytes());
                target.checksum_xor ^= checksum;
                if target.is_pure() {
                    queue.push_back(bucket);
                }
            }
        }

        if working.cells.iter().all(|cell| cell.is_zero()) {
            Ok(ReconciliationDelta {
                missing_locally,
                missing_remotely,
            })
        } else {
            let residual_cells = working.cells.iter().filter(|cell| !cell.is_zero()).count();
            Err(IbltError::PeelOverflow { residual_cells })
        }
    }
}

/// Reconcile object-id sets via IBLT; fall back to segment-hash scan on failure.
#[must_use]
pub fn reconcile_object_id_sets(
    local: &BTreeSet<ObjectId>,
    remote: &BTreeSet<ObjectId>,
    iblt_cell_count: usize,
) -> ReconciliationResult {
    debug!(
        bead_id = PERMEATION_BEAD_ID,
        logging_standard = PERMEATION_LOGGING_STANDARD,
        local_count = local.len(),
        remote_count = remote.len(),
        iblt_cell_count = iblt_cell_count,
        "starting object-id anti-entropy reconciliation"
    );

    let mut local_iblt = match ObjectIdIblt::from_set(local, iblt_cell_count) {
        Ok(iblt) => iblt,
        Err(new_error) => {
            warn!(
                bead_id = PERMEATION_BEAD_ID,
                logging_standard = PERMEATION_LOGGING_STANDARD,
                error = %new_error,
                "invalid IBLT configuration; degrading to segment-hash fallback"
            );
            return segment_hash_scan_fallback(local, remote);
        }
    };
    let remote_iblt = match ObjectIdIblt::from_set(remote, iblt_cell_count) {
        Ok(iblt) => iblt,
        Err(new_error) => {
            warn!(
                bead_id = PERMEATION_BEAD_ID,
                logging_standard = PERMEATION_LOGGING_STANDARD,
                error = %new_error,
                "invalid remote IBLT configuration; degrading to segment-hash fallback"
            );
            return segment_hash_scan_fallback(local, remote);
        }
    };

    if let Err(subtract_error) = local_iblt.subtract_assign(&remote_iblt) {
        warn!(
            bead_id = PERMEATION_BEAD_ID,
            logging_standard = PERMEATION_LOGGING_STANDARD,
            error = %subtract_error,
            "IBLT subtraction failed; degrading to segment-hash fallback"
        );
        return segment_hash_scan_fallback(local, remote);
    }

    match local_iblt.peel() {
        Ok(delta) => {
            info!(
                bead_id = PERMEATION_BEAD_ID,
                logging_standard = PERMEATION_LOGGING_STANDARD,
                missing_locally = delta.missing_locally.len(),
                missing_remotely = delta.missing_remotely.len(),
                "IBLT reconciliation completed"
            );
            ReconciliationResult {
                delta,
                used_fallback: false,
            }
        }
        Err(peel_error) => {
            warn!(
                bead_id = PERMEATION_BEAD_ID,
                logging_standard = PERMEATION_LOGGING_STANDARD,
                error = %peel_error,
                "IBLT peel overflow; degrading to segment-hash fallback"
            );
            segment_hash_scan_fallback(local, remote)
        }
    }
}

/// Fallback reconciliation with deterministic segment-hash scan.
#[must_use]
pub fn segment_hash_scan_fallback(
    local: &BTreeSet<ObjectId>,
    remote: &BTreeSet<ObjectId>,
) -> ReconciliationResult {
    let missing_locally: BTreeSet<ObjectId> = remote.difference(local).copied().collect();
    let missing_remotely: BTreeSet<ObjectId> = local.difference(remote).copied().collect();

    info!(
        bead_id = PERMEATION_BEAD_ID,
        logging_standard = PERMEATION_LOGGING_STANDARD,
        missing_locally = missing_locally.len(),
        missing_remotely = missing_remotely.len(),
        "segment-hash fallback reconciliation completed"
    );

    ReconciliationResult {
        delta: ReconciliationDelta {
            missing_locally,
            missing_remotely,
        },
        used_fallback: true,
    }
}

fn parse_symbol_size_policy(raw: &str) -> Result<SymbolSizePolicy, SymbolPolicyParseError> {
    if raw == "T=page_size" {
        return Ok(SymbolSizePolicy::PageSize);
    }

    if let Some(inner) = raw
        .strip_prefix("T=min(page_size,")
        .and_then(|value| value.strip_suffix(')'))
    {
        let cap = inner
            .parse::<u32>()
            .map_err(|_| SymbolPolicyParseError::new(format!("invalid min() cap: {raw}")))?;
        return Ok(SymbolSizePolicy::MinPageSize { cap_bytes: cap });
    }

    if let Some(bytes) = raw
        .strip_prefix("T=")
        .and_then(|value| value.strip_suffix('B'))
    {
        if let Some((lo, hi)) = bytes.split_once('-') {
            let min_bytes = lo.parse::<u32>().map_err(|_| {
                SymbolPolicyParseError::new(format!("invalid range lower bound: {raw}"))
            })?;
            let max_bytes = hi.parse::<u32>().map_err(|_| {
                SymbolPolicyParseError::new(format!("invalid range upper bound: {raw}"))
            })?;
            if min_bytes > max_bytes {
                return Err(SymbolPolicyParseError::new(format!(
                    "range lower bound exceeds upper bound: {raw}"
                )));
            }
            return Ok(SymbolSizePolicy::RangeBytes {
                min_bytes,
                max_bytes,
            });
        }

        let fixed_bytes = bytes.parse::<u32>().map_err(|_| {
            SymbolPolicyParseError::new(format!("invalid fixed symbol size: {raw}"))
        })?;
        return Ok(SymbolSizePolicy::FixedBytes(fixed_bytes));
    }

    Err(SymbolPolicyParseError::new(format!(
        "unsupported symbol-size policy: {raw}"
    )))
}

fn parse_redundancy_policy(raw: &str) -> Result<RedundancyPolicy, SymbolPolicyParseError> {
    let normalized = raw.strip_suffix(" default").map_or(raw, str::trim).trim();
    match normalized {
        "policy-driven" => Ok(RedundancyPolicy::PolicyDriven),
        "per-group" => Ok(RedundancyPolicy::PerGroup),
        "transport-policy" => Ok(RedundancyPolicy::TransportPolicy),
        _ => {
            let bps = parse_percent_bps(normalized).ok_or_else(|| {
                SymbolPolicyParseError::new(format!("invalid redundancy policy: {raw}"))
            })?;
            Ok(RedundancyPolicy::PercentBps(bps))
        }
    }
}

fn parse_percent_bps(raw: &str) -> Option<u16> {
    let percent = raw.strip_suffix('%')?;
    let (whole_raw, frac_raw) = percent.split_once('.').unwrap_or((percent, ""));
    let whole = whole_raw.parse::<u16>().ok()?;
    let frac_bps = if frac_raw.is_empty() {
        0_u16
    } else if frac_raw.len() == 1 {
        frac_raw
            .chars()
            .next()
            .and_then(|ch| ch.to_digit(10))
            .and_then(|digit| u16::try_from(digit).ok())
            .map_or(0, |digit| digit * 10)
    } else if frac_raw.len() == 2 {
        frac_raw.parse::<u16>().ok()?
    } else {
        return None;
    };

    let bps = whole.checked_mul(100)?.checked_add(frac_bps)?;
    if bps > 10_000 { None } else { Some(bps) }
}

fn parse_fixed_policy(raw: &str) -> Option<(u32, &str)> {
    let fixed = raw.strip_prefix("fixed:")?;
    let (bytes_raw, rest) = fixed.split_once('B')?;
    let bytes = bytes_raw.parse::<u32>().ok()?;
    let redundancy = rest.split_once(", R=").map_or("0%", |(_, r)| r);
    Some((bytes, redundancy.trim()))
}

fn xor_in_place(target: &mut [u8; 16], rhs: &[u8; 16]) {
    for (left, right) in target.iter_mut().zip(rhs.iter()) {
        *left ^= *right;
    }
}

fn checksum_for_bytes(bytes: &[u8; 16]) -> u32 {
    let mut state = 0x811C_9DC5_u32;
    for byte in bytes {
        state ^= u32::from(*byte);
        state = state.wrapping_mul(0x0100_0193);
    }
    state
}

fn bucket_indices(object_id: ObjectId, cell_count: usize) -> [usize; IBLT_HASH_COUNT] {
    let mut out = [0_usize; IBLT_HASH_COUNT];
    let modulus = match u64::try_from(cell_count) {
        Ok(value) => value.max(1),
        Err(_) => u64::MAX,
    };

    for (slot, seed) in IBLT_HASH_SEEDS.iter().enumerate() {
        let hash = seeded_object_hash(object_id.as_bytes(), *seed);
        let index_u64 = hash % modulus;
        out[slot] = usize::try_from(index_u64).unwrap_or(0);
    }
    out
}

fn seeded_object_hash(object_id: &[u8; 16], seed: u64) -> u64 {
    let mut first = [0_u8; 8];
    let mut second = [0_u8; 8];
    first.copy_from_slice(&object_id[..8]);
    second.copy_from_slice(&object_id[8..]);

    let a = u64::from_le_bytes(first);
    let b = u64::from_le_bytes(second);

    let mut x = seed
        ^ a.wrapping_mul(0x9E37_79B1_85EB_CA87)
        ^ b.rotate_left(17).wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
    x ^= x >> 33;
    x = x.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
    x ^= x >> 33;
    x = x.wrapping_mul(0xC4CE_B9FE_1A85_EC53);
    x ^ (x >> 33)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oid_from_u64(value: u64) -> ObjectId {
        let mut bytes = [0_u8; 16];
        bytes[..8].copy_from_slice(&value.to_le_bytes());
        bytes[8..].copy_from_slice(&(!value).to_le_bytes());
        ObjectId::from_bytes(bytes)
    }

    fn find_entry(subsystem: &str) -> &'static PermeationEntry {
        PERMEATION_MAP
            .iter()
            .find(|entry| entry.subsystem == subsystem)
            .expect("expected subsystem entry")
    }

    #[test]
    fn test_permeation_map_complete() {
        assert!(!PERMEATION_MAP.is_empty());
        for required in V1_REQUIRED_SUBSYSTEMS {
            assert!(
                PERMEATION_MAP
                    .iter()
                    .any(|entry| entry.subsystem == *required),
                "missing required subsystem: {required}"
            );
        }

        for entry in PERMEATION_MAP {
            assert!(!entry.subsystem.is_empty());
            assert!(!entry.object_type.is_empty());
            assert!(!entry.symbol_size_policy.is_empty());
            assert!(!entry.repair_story.is_empty());
        }
    }

    #[test]
    fn test_permeation_map_no_gaps() {
        let failures = audit_permeation_map();
        assert!(failures.is_empty(), "unexpected gaps: {failures:#?}");
    }

    #[test]
    fn test_permeation_map_no_duplicates() {
        let mut seen = BTreeSet::new();
        for entry in PERMEATION_MAP {
            assert!(
                seen.insert((entry.plane, entry.subsystem)),
                "duplicate subsystem '{}' in plane {:?}",
                entry.subsystem,
                entry.plane
            );
        }
    }

    #[test]
    fn test_permeation_map_symbol_policy_parseable() {
        for page_size in [1024_u32, 4096, 65536] {
            for entry in PERMEATION_MAP {
                let parsed = parse_symbol_policy(entry.symbol_size_policy)
                    .expect("symbol policy must be parseable");
                let resolved = parsed.resolve(page_size, PolicyResolutionDefaults::default());
                assert!(resolved.symbol_size_bytes >= 16);
                assert!(resolved.redundancy_bps <= 10_000);
            }
        }
    }

    #[test]
    fn test_permeation_map_commit_capsule_policy() {
        let entry = find_entry("Commits/CapsuleProof");
        let parsed = parse_symbol_policy(entry.symbol_size_policy).expect("parse");
        let defaults = PolicyResolutionDefaults::default();

        let resolved_4096 = parsed.resolve(4096, defaults);
        assert_eq!(resolved_4096.symbol_size_bytes, 4096);
        assert_eq!(resolved_4096.redundancy_bps, 2_000);

        let resolved_65536 = parsed.resolve(65536, defaults);
        assert_eq!(resolved_65536.symbol_size_bytes, 4096);
        assert_eq!(resolved_65536.redundancy_bps, 2_000);
    }

    #[test]
    fn test_permeation_map_page_history_policy() {
        let entry = find_entry("Page storage");
        let parsed = parse_symbol_policy(entry.symbol_size_policy).expect("parse");
        let resolved = parsed.resolve(4096, PolicyResolutionDefaults::default());
        assert_eq!(resolved.symbol_size_bytes, 4096);
        assert_eq!(resolved.redundancy_bps, 2_000);
    }

    #[test]
    fn test_permeation_map_marker_record_policy() {
        let entry = find_entry("Commits/MarkerStream");
        let parsed = parse_symbol_policy(entry.symbol_size_policy).expect("parse");
        let resolved = parsed.resolve(4096, PolicyResolutionDefaults::default());
        assert_eq!(resolved.symbol_size_bytes, 88);
        assert_eq!(resolved.redundancy_bps, 0);
        assert!(!resolved.fountain_coded);
    }

    #[test]
    fn test_iblt_set_reconciliation() {
        let local: BTreeSet<ObjectId> = (0_u64..100).map(oid_from_u64).collect();
        let remote: BTreeSet<ObjectId> = (5_u64..105).map(oid_from_u64).collect();
        let result = reconcile_object_id_sets(&local, &remote, 128);

        assert!(!result.used_fallback, "expected IBLT to peel successfully");
        assert_eq!(result.delta.missing_locally.len(), 5);
        assert_eq!(result.delta.missing_remotely.len(), 5);
        assert_eq!(
            result.delta.missing_locally.len() + result.delta.missing_remotely.len(),
            10
        );
    }

    #[test]
    fn test_iblt_fallback_on_overflow() {
        let local: BTreeSet<ObjectId> = (0_u64..300).map(oid_from_u64).collect();
        let remote: BTreeSet<ObjectId> = (300_u64..600).map(oid_from_u64).collect();
        let result = reconcile_object_id_sets(&local, &remote, 8);

        assert!(result.used_fallback, "expected overflow fallback");
        assert_eq!(result.delta.missing_locally.len(), 300);
        assert_eq!(result.delta.missing_remotely.len(), 300);
    }

    #[test]
    fn test_audit_no_gaps() {
        let failures = audit_permeation_map();
        assert!(
            failures.is_empty(),
            "expected no audit failures: {failures:#?}"
        );
    }

    #[test]
    fn test_audit_new_subsystem_requires_entry() {
        let mut required = V1_REQUIRED_SUBSYSTEMS.to_vec();
        required.push("Future storage lane");
        let failures = audit_permeation_entries(
            PERMEATION_MAP,
            &required,
            4096,
            PolicyResolutionDefaults::default(),
        );
        assert!(
            failures
                .iter()
                .any(|failure| failure.kind == AuditFailureKind::MissingEntry
                    && failure.subsystem == "Future storage lane")
        );
    }

    #[test]
    fn test_bd_1hi_27_unit_compliance_gate() {
        assert_eq!(PERMEATION_BEAD_ID, "bd-1hi.27");
        assert_eq!(PERMEATION_LOGGING_STANDARD, "bd-1fpm");
        assert!(audit_permeation_map().is_empty());
    }

    #[test]
    fn prop_bd_1hi_27_structure_compliance() {
        let required_planes = [
            Plane::Durability,
            Plane::Concurrency,
            Plane::Replication,
            Plane::Observability,
        ];
        for plane in required_planes {
            assert!(
                PERMEATION_MAP.iter().any(|entry| entry.plane == plane),
                "missing plane entry: {plane:?}"
            );
        }

        for page_size in [512_u32, 1024, 2048, 4096, 8192, 16384, 65536] {
            for entry in PERMEATION_MAP {
                let parsed = parse_symbol_policy(entry.symbol_size_policy).expect("parse");
                let resolved = parsed.resolve(page_size, PolicyResolutionDefaults::default());
                assert!(resolved.symbol_size_bytes >= 16);
                assert!(resolved.redundancy_bps <= 10_000);
            }
        }
    }

    #[test]
    fn test_e2e_bd_1hi_27_compliance() {
        let failures = audit_permeation_map();
        assert!(failures.is_empty(), "audit should pass in e2e");

        let local: BTreeSet<ObjectId> = (0_u64..64).map(oid_from_u64).collect();
        let remote: BTreeSet<ObjectId> = (32_u64..96).map(oid_from_u64).collect();
        let iblt_ok = reconcile_object_id_sets(&local, &remote, 192);
        assert!(!iblt_ok.used_fallback);
        assert_eq!(
            iblt_ok.delta.missing_locally.len() + iblt_ok.delta.missing_remotely.len(),
            64
        );

        let overflow = reconcile_object_id_sets(&local, &remote, 4);
        assert!(overflow.used_fallback);
        let artifact = format!(
            "bead={} log={} iblt_ok={} fallback={}",
            PERMEATION_BEAD_ID,
            PERMEATION_LOGGING_STANDARD,
            !iblt_ok.used_fallback,
            overflow.used_fallback
        );
        assert!(artifact.contains("bd-1hi.27"));
        assert!(artifact.contains("bd-1fpm"));
    }
}
