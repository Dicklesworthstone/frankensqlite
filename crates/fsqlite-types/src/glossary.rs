//! Glossary types (§0.3).
//!
//! This module defines (or re-exports) the core cross-cutting types referenced
//! throughout the FrankenSQLite specification: MVCC identifiers, SSI witness
//! keys, and ECS content-addressed identities.

use std::fmt;
use std::num::NonZeroU64;

use crate::encoding::{
    append_u16_le, append_u32_le, append_u64_le, read_u16_le, read_u32_le, read_u64_le,
};
use crate::{ObjectId, PageData, PageNumber};

/// Monotonically increasing transaction identifier.
///
/// Domain: `1..=(2^62 - 1)`.
///
/// The top two bits are reserved for TxnSlot sentinel encoding (CLAIMING /
/// CLEANING) per §5.6.2; sentinel values are *not* represented as `TxnId`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[repr(transparent)]
pub struct TxnId(NonZeroU64);

impl TxnId {
    /// Maximum raw value representable by a real transaction id.
    pub const MAX_RAW: u64 = (1_u64 << 62) - 1;

    /// Construct a `TxnId` if `raw` is in-domain.
    #[inline]
    pub const fn new(raw: u64) -> Option<Self> {
        if raw == 0 || raw > Self::MAX_RAW {
            return None;
        }
        match NonZeroU64::new(raw) {
            Some(nz) => Some(Self(nz)),
            None => None,
        }
    }

    /// Get the raw u64 value.
    #[inline]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// Return the next transaction id if it stays in-domain.
    #[inline]
    pub const fn checked_next(self) -> Option<Self> {
        Self::new(self.get().wrapping_add(1))
    }
}

impl fmt::Display for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "txn#{}", self.get())
    }
}

impl TryFrom<u64> for TxnId {
    type Error = InvalidTxnId;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value).ok_or(InvalidTxnId { raw: value })
    }
}

/// Error returned when attempting to construct an out-of-domain `TxnId`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidTxnId {
    raw: u64,
}

impl fmt::Display for InvalidTxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid TxnId {} (must satisfy 1 <= id <= {})",
            self.raw,
            TxnId::MAX_RAW
        )
    }
}

impl std::error::Error for InvalidTxnId {}

/// Monotonically increasing global commit sequence number ("commit clock").
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[repr(transparent)]
pub struct CommitSeq(u64);

impl CommitSeq {
    pub const ZERO: Self = Self(0);

    #[inline]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }

    #[inline]
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.wrapping_add(1))
    }
}

impl fmt::Display for CommitSeq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cs#{}", self.get())
    }
}

/// Per-transaction epoch used to disambiguate slot reuse across crashes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[repr(transparent)]
pub struct TxnEpoch(u32);

impl TxnEpoch {
    #[inline]
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// A stable transaction identity pair: (TxnId, TxnEpoch).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TxnToken {
    pub id: TxnId,
    pub epoch: TxnEpoch,
}

impl TxnToken {
    #[inline]
    pub const fn new(id: TxnId, epoch: TxnEpoch) -> Self {
        Self { id, epoch }
    }
}

/// Monotonically increasing schema epoch (invalidates prepared statements).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[repr(transparent)]
pub struct SchemaEpoch(u64);

impl SchemaEpoch {
    pub const ZERO: Self = Self(0);

    #[inline]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A frozen view of the database at BEGIN time.
///
/// Visibility check is a single integer comparison: `version.commit_seq <= snapshot.high`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Snapshot {
    pub high: CommitSeq,
    pub schema_epoch: SchemaEpoch,
}

impl Snapshot {
    #[inline]
    pub const fn new(high: CommitSeq, schema_epoch: SchemaEpoch) -> Self {
        Self { high, schema_epoch }
    }
}

/// Opaque pointer to a previous page version in a version chain.
///
/// In the implementation this is expected to be an arena index or object
/// locator, not a raw pointer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[repr(transparent)]
pub struct VersionPointer(u64);

impl VersionPointer {
    #[inline]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A single committed version of a database page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageVersion {
    pub pgno: PageNumber,
    pub commit_seq: CommitSeq,
    pub created_by: TxnToken,
    pub data: PageData,
    pub prev: Option<VersionPointer>,
}

/// A commit capsule is the durable ECS object that commits refer to.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct CommitCapsule {
    pub object_id: ObjectId,
}

/// Commit marker persisted in the commit chain.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct CommitMarker {
    pub commit_seq: CommitSeq,
    pub commit_time_unix_ns: u64,
    pub capsule_object_id: ObjectId,
    pub proof_object_id: ObjectId,
    pub prev_marker: Option<ObjectId>,
    pub integrity_hash: [u8; 32],
}

/// Object Transmission Information (RaptorQ / RFC 6330).
///
/// This is an internal encoding, NOT the RFC 6330 Common FEC OTI wire format.
/// Field widths are widened for implementation convenience:
/// - `f` is `u64` (RFC: 40-bit)
/// - `t` is `u32` (RFC: 16-bit) -- supports `page_size = 65_536`
/// - `z` is `u32` (RFC: 12-bit)
/// - `n` is `u32` (RFC: 8-bit)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Oti {
    /// Transfer length (bytes).
    pub f: u64,
    /// Alignment parameter.
    pub al: u16,
    /// Symbol size (bytes). `u32` to represent all valid SQLite page sizes.
    pub t: u32,
    /// Number of source blocks.
    pub z: u32,
    /// Number of sub-blocks.
    pub n: u32,
}

/// Serialized size of [`Oti`] on the wire: `8 + 2 + 4 + 4 + 4 = 22` bytes.
pub const OTI_WIRE_SIZE: usize = 22;

impl Oti {
    /// Serialize to canonical little-endian bytes.
    #[must_use]
    pub fn to_bytes(self) -> [u8; OTI_WIRE_SIZE] {
        let mut as_vec = Vec::with_capacity(OTI_WIRE_SIZE);
        append_u64_le(&mut as_vec, self.f);
        append_u16_le(&mut as_vec, self.al);
        append_u32_le(&mut as_vec, self.t);
        append_u32_le(&mut as_vec, self.z);
        append_u32_le(&mut as_vec, self.n);

        let mut buf = [0u8; OTI_WIRE_SIZE];
        buf.copy_from_slice(&as_vec);
        buf
    }

    /// Deserialize from canonical little-endian bytes.
    ///
    /// Returns `None` if `data` is shorter than [`OTI_WIRE_SIZE`].
    #[must_use]
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < OTI_WIRE_SIZE {
            return None;
        }
        Some(Self {
            f: read_u64_le(&data[0..8])?,
            al: read_u16_le(&data[8..10])?,
            t: read_u32_le(&data[10..14])?,
            z: read_u32_le(&data[14..18])?,
            n: read_u32_le(&data[18..22])?,
        })
    }
}

/// Proof that a decode was correct (structure depends on codec mode).
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct DecodeProof {
    pub object_id: ObjectId,
    pub oti: Oti,
}

/// Capability context + cooperative budget types.
///
/// Canonical definitions live in `crate::cx` (per `bd-3go.1`).
pub use crate::cx::{Budget, Cx};

/// Result outcome lattice for cooperative cancellation and failure.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum Outcome {
    Ok,
    Err,
    Cancelled,
    Panicked,
}

/// Global epoch identifier (monotonically increasing).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[repr(transparent)]
pub struct EpochId(u64);

impl EpochId {
    #[inline]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Validity window for symbols or proofs (inclusive bounds).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct SymbolValidityWindow {
    pub from_epoch: EpochId,
    pub to_epoch: EpochId,
}

impl SymbolValidityWindow {
    #[must_use]
    pub const fn new(from_epoch: EpochId, to_epoch: EpochId) -> Self {
        Self {
            from_epoch,
            to_epoch,
        }
    }
}

/// Capability token authorizing access to a remote endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[repr(transparent)]
pub struct RemoteCap([u8; 16]);

/// Capability token for the symbol authentication master key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[repr(transparent)]
pub struct SymbolAuthMasterKeyCap([u8; 32]);

/// Stable idempotency key for retry-safe operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[repr(transparent)]
pub struct IdempotencyKey([u8; 16]);

/// Saga identifier (ties together a multi-step idempotent workflow).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Saga {
    pub key: IdempotencyKey,
}

/// Logical region identifier (tiering / placement / replication scope).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[repr(transparent)]
pub struct Region(u32);

impl Region {
    #[inline]
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// SSI witness key basis.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum WitnessKey {
    /// Coarse witness: entire page.
    Page(PageNumber),
    /// Semantic witness: specific B-tree cell/tag.
    Cell { btree_root: PageNumber, tag: u64 },
    /// Semantic witness: structured byte range on a page.
    ByteRange {
        page: PageNumber,
        start: u32,
        len: u32,
    },
}

/// Witness hierarchy range key (prefix-based bucketing).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct RangeKey {
    pub level: u8,
    pub hash_prefix: u32,
}

/// A recorded SSI read witness.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ReadWitness {
    pub txn: TxnId,
    pub key: WitnessKey,
}

/// A recorded SSI write witness.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct WriteWitness {
    pub txn: TxnId,
    pub key: WitnessKey,
}

/// A persisted segment of witness index updates.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WitnessIndexSegment {
    pub epoch: EpochId,
    pub reads: Vec<ReadWitness>,
    pub writes: Vec<WriteWitness>,
}

/// A dependency edge in the SSI serialization graph.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct DependencyEdge {
    pub from: TxnId,
    pub to: TxnId,
    pub key_basis: WitnessKey,
    pub observed_by: TxnId,
}

/// Proof object tying together the dependency edges relevant to a commit decision.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CommitProof {
    pub edges: Vec<DependencyEdge>,
}

/// Identifier for a table b-tree root (logical, not physical file page).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[repr(transparent)]
pub struct TableId(u32);

impl TableId {
    #[inline]
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Identifier for an index b-tree root (logical, not physical file page).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[repr(transparent)]
pub struct IndexId(u32);

impl IndexId {
    #[inline]
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// RowId / INTEGER PRIMARY KEY key space (SQLite uses signed 64-bit).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[repr(transparent)]
pub struct RowId(i64);

impl RowId {
    /// Maximum RowId value: 2^63 - 1.
    pub const MAX: Self = Self(i64::MAX);

    #[inline]
    pub const fn new(raw: i64) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn get(self) -> i64 {
        self.0
    }
}

/// Rowid allocation mode for a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RowIdMode {
    /// Normal rowid: max(rowid)+1, deleted rowids may be reused.
    Normal,
    /// AUTOINCREMENT: never reuse deleted rowids. Uses sqlite_sequence
    /// high-water mark. Returns error at MAX_ROWID.
    AutoIncrement,
}

/// Rowid allocator implementing SQLite's allocation semantics.
///
/// - Normal mode: next rowid = max(existing) + 1. Deleted rowids may be reused
///   when max rowid is not the table maximum.
/// - AUTOINCREMENT mode: next rowid = max(max_existing, sqlite_sequence) + 1.
///   Rowids are never reused. When MAX_ROWID is reached, allocation fails.
#[derive(Debug, Clone)]
pub struct RowIdAllocator {
    mode: RowIdMode,
    /// High-water mark from sqlite_sequence (AUTOINCREMENT only).
    sequence_high_water: i64,
}

impl RowIdAllocator {
    /// Create a new allocator.
    pub const fn new(mode: RowIdMode) -> Self {
        Self {
            mode,
            sequence_high_water: 0,
        }
    }

    /// Allocate the next rowid given the current maximum rowid in the table.
    ///
    /// `max_existing` is `None` if the table is empty.
    ///
    /// Returns `Ok(rowid)` or `Err` if MAX_ROWID is exhausted (AUTOINCREMENT only).
    pub fn allocate(&mut self, max_existing: Option<RowId>) -> Result<RowId, RowIdExhausted> {
        let max_val = max_existing.map_or(0, RowId::get);

        match self.mode {
            RowIdMode::Normal => {
                if max_val < i64::MAX {
                    Ok(RowId::new(max_val + 1))
                } else {
                    // MAX_ROWID reached: SQLite tries random probing.
                    // For the type-level implementation, we signal exhaustion.
                    Err(RowIdExhausted)
                }
            }
            RowIdMode::AutoIncrement => {
                let base = max_val.max(self.sequence_high_water);
                if base == i64::MAX {
                    return Err(RowIdExhausted);
                }
                let next = base + 1;
                self.sequence_high_water = next;
                Ok(RowId::new(next))
            }
        }
    }

    /// Get the current sqlite_sequence high-water mark.
    pub const fn sequence_high_water(&self) -> i64 {
        self.sequence_high_water
    }

    /// Set the sqlite_sequence high-water mark (loaded from DB).
    pub fn set_sequence_high_water(&mut self, val: i64) {
        self.sequence_high_water = val;
    }
}

/// Error when rowid space is exhausted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowIdExhausted;

impl std::fmt::Display for RowIdExhausted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("database or object is full (rowid exhausted)")
    }
}

/// Column index within a table (0-based).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[repr(transparent)]
pub struct ColumnIdx(u32);

impl ColumnIdx {
    #[inline]
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn get(self) -> u32 {
        self.0
    }
}

// ---------------------------------------------------------------------------
// §5.10.1 Intent Logs — Semantic Operations + Footprints
// ---------------------------------------------------------------------------

/// Reference to a B-tree (either table or index).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum BtreeRef {
    Table(TableId),
    Index(IndexId),
}

/// Kind of semantic key reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum SemanticKeyKind {
    TableRow,
    IndexEntry,
}

/// Semantic key reference with a stable BLAKE3-based digest.
///
/// `key_digest = Trunc128(BLAKE3("fsqlite:btree:key:v1" || kind || btree_id || canonical_key_bytes))`
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct SemanticKeyRef {
    pub btree: BtreeRef,
    pub kind: SemanticKeyKind,
    pub key_digest: [u8; 16],
}

impl SemanticKeyRef {
    /// Domain separation prefix for the key digest.
    const DOMAIN_SEP: &'static [u8] = b"fsqlite:btree:key:v1";

    /// Compute the key digest from kind, btree id, and canonical key bytes.
    #[must_use]
    pub fn compute_digest(
        kind: SemanticKeyKind,
        btree: BtreeRef,
        canonical_key_bytes: &[u8],
    ) -> [u8; 16] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(Self::DOMAIN_SEP);
        hasher.update(&[match kind {
            SemanticKeyKind::TableRow => 0,
            SemanticKeyKind::IndexEntry => 1,
        }]);
        match btree {
            BtreeRef::Table(id) => {
                hasher.update(&[0]);
                hasher.update(&id.get().to_le_bytes());
            }
            BtreeRef::Index(id) => {
                hasher.update(&[1]);
                hasher.update(&id.get().to_le_bytes());
            }
        }
        hasher.update(canonical_key_bytes);
        let hash = hasher.finalize();
        let bytes = hash.as_bytes();
        let mut digest = [0u8; 16];
        digest.copy_from_slice(&bytes[..16]);
        digest
    }

    /// Construct a `SemanticKeyRef` by computing the digest.
    #[must_use]
    pub fn new(btree: BtreeRef, kind: SemanticKeyKind, canonical_key_bytes: &[u8]) -> Self {
        let key_digest = Self::compute_digest(kind, btree, canonical_key_bytes);
        Self {
            btree,
            kind,
            key_digest,
        }
    }
}

bitflags::bitflags! {
    /// Structural side effects that make operations non-commutative.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct StructuralEffects: u32 {
        /// No structural effects (simple leaf operations).
        const NONE = 0;
        /// A B-tree page was split.
        const PAGE_SPLIT = 1;
        /// A B-tree page was merged.
        const PAGE_MERGE = 2;
        /// Multi-page balance operation.
        const BALANCE_MULTI_PAGE = 4;
        /// An overflow page was allocated.
        const OVERFLOW_ALLOC = 8;
        /// An overflow chain was mutated.
        const OVERFLOW_MUTATE = 16;
        /// The freelist was modified.
        const FREELIST_MUTATE = 32;
        /// The pointer map was modified.
        const POINTER_MAP_MUTATE = 64;
        /// Cells were moved during defragmentation.
        const DEFRAG_MOVE_CELLS = 128;
    }
}

impl serde::Serialize for StructuralEffects {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.bits().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for StructuralEffects {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bits = u32::deserialize(deserializer)?;
        Self::from_bits(bits).ok_or_else(|| {
            serde::de::Error::custom(format!("invalid StructuralEffects bits: {bits:#x}"))
        })
    }
}

impl Default for StructuralEffects {
    fn default() -> Self {
        Self::NONE
    }
}

/// Semantic read/write footprint of an intent operation (§5.10.1).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IntentFootprint {
    pub reads: Vec<SemanticKeyRef>,
    pub writes: Vec<SemanticKeyRef>,
    pub structural: StructuralEffects,
}

impl IntentFootprint {
    /// Create an empty footprint with no effects.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            reads: Vec::new(),
            writes: Vec::new(),
            structural: StructuralEffects::NONE,
        }
    }
}

impl Default for IntentFootprint {
    fn default() -> Self {
        Self::empty()
    }
}

/// Replayable expression AST for deterministic rebase (§5.10.1).
///
/// Allowed forms are intentionally strict: only proven-deterministic
/// expressions may appear. Enforced by `expr_is_rebase_safe()`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum RebaseExpr {
    /// Reference to a column in the current row.
    ColumnRef(ColumnIdx),
    /// A literal value.
    Literal(crate::SqliteValue),
    /// A unary operation.
    UnaryOp {
        op: RebaseUnaryOp,
        operand: Box<Self>,
    },
    /// A binary operation.
    BinaryOp {
        op: RebaseBinaryOp,
        left: Box<Self>,
        right: Box<Self>,
    },
    /// A deterministic function call.
    FunctionCall { name: String, args: Vec<Self> },
    /// CAST(expr AS type).
    Cast { expr: Box<Self>, type_name: String },
    /// CASE WHEN ... THEN ... ELSE ... END.
    Case {
        operand: Option<Box<Self>>,
        when_clauses: Vec<(Self, Self)>,
        else_clause: Option<Box<Self>>,
    },
    /// COALESCE(expr, expr, ...).
    Coalesce(Vec<Self>),
    /// NULLIF(expr, expr).
    NullIf { left: Box<Self>, right: Box<Self> },
    /// String concatenation (||).
    Concat { left: Box<Self>, right: Box<Self> },
}

/// Unary operators allowed in rebase expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum RebaseUnaryOp {
    Negate,
    BitwiseNot,
    Not,
}

/// Binary operators allowed in rebase expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum RebaseBinaryOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Remainder,
    BitwiseAnd,
    BitwiseOr,
    ShiftLeft,
    ShiftRight,
}

/// The kind of semantic operation in an intent log entry.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum IntentOpKind {
    Insert {
        table: TableId,
        key: RowId,
        record: Vec<u8>,
    },
    Delete {
        table: TableId,
        key: RowId,
    },
    Update {
        table: TableId,
        key: RowId,
        new_record: Vec<u8>,
    },
    IndexInsert {
        index: IndexId,
        key: Vec<u8>,
        rowid: RowId,
    },
    IndexDelete {
        index: IndexId,
        key: Vec<u8>,
        rowid: RowId,
    },
    /// Column-level rebase expressions for deterministic rebase (§5.10.1).
    UpdateExpression {
        table: TableId,
        key: RowId,
        column_updates: Vec<(ColumnIdx, RebaseExpr)>,
    },
}

/// A single entry in the transaction intent log (§5.10.1).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct IntentOp {
    pub schema_epoch: u64,
    pub footprint: IntentFootprint,
    pub op: IntentOpKind,
}

/// Transaction intent log: an ordered sequence of semantic operations.
pub type IntentLog = Vec<IntentOp>;

/// History of versions for a page, used by debugging and invariant checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageHistory {
    pub pgno: PageNumber,
    pub versions: Vec<PageVersion>,
}

/// ARC cache placeholder type (Adaptive Replacement Cache).
///
/// The actual ARC algorithm lives in `fsqlite-pager`; this type exists to keep
/// glossary terminology stable across crates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ArcCache;

/// Root manifest tying together the durable roots of the database state.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RootManifest {
    pub schema_epoch: SchemaEpoch,
    pub root_page: PageNumber,
}

/// Transaction slot index (cross-process shared memory slot).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[repr(transparent)]
pub struct TxnSlot(u32);

impl TxnSlot {
    #[inline]
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn get(self) -> u32 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::Duration;

    use proptest::prelude::*;

    use crate::PayloadHash;

    use super::*;

    #[test]
    fn test_txn_id_nonzero_enforced() {
        assert!(TxnId::new(0).is_none());
        assert!(TxnId::try_from(0_u64).is_err());
        assert!(TxnId::new(1).is_some());
        assert!(TxnId::new(TxnId::MAX_RAW).is_some());
    }

    #[test]
    fn test_txn_id_62_bit_max() {
        assert!(TxnId::new(TxnId::MAX_RAW + 1).is_none());
        assert!(TxnId::try_from(TxnId::MAX_RAW + 1).is_err());
    }

    #[test]
    fn test_object_id_16_bytes_blake3_truncation() {
        let header = b"hdr:v1";
        let payload = b"payload";
        let oid = ObjectId::derive(header, PayloadHash::blake3(payload));
        assert_eq!(oid.as_bytes().len(), ObjectId::LEN);
    }

    #[test]
    fn test_object_id_content_addressed() {
        let header = b"hdr:v1";
        let payload = b"payload";
        let a = ObjectId::derive(header, PayloadHash::blake3(payload));
        let b = ObjectId::derive(header, PayloadHash::blake3(payload));
        assert_eq!(a, b);

        let c = ObjectId::derive(header, PayloadHash::blake3(b"payload2"));
        assert_ne!(a, c);
    }

    #[test]
    fn prop_object_id_collision_resistance() {
        let header = b"hdr:v1";
        let mut ids = HashSet::<ObjectId>::with_capacity(10_000);

        let mut state: u64 = 0xD6E8_FEB8_6659_FD93;
        for i in 0..10_000_u64 {
            // Deterministic pseudo-randomness, but ensure distinct inputs by embedding i.
            state = state
                .wrapping_mul(6_364_136_223_846_793_005_u64)
                .wrapping_add(1_442_695_040_888_963_407_u64);

            let mut payload = [0_u8; 32];
            payload[..8].copy_from_slice(&i.to_le_bytes());
            payload[8..16].copy_from_slice(&state.to_le_bytes());
            payload[16..24].copy_from_slice(&state.rotate_left(17).to_le_bytes());
            payload[24..32].copy_from_slice(&state.rotate_left(41).to_le_bytes());

            let oid = ObjectId::derive(header, PayloadHash::blake3(&payload));
            assert!(ids.insert(oid), "ObjectId collision at i={i}");
        }
    }

    #[test]
    fn test_snapshot_fields() {
        let snap = Snapshot::new(CommitSeq::new(7), SchemaEpoch::new(9));
        assert_eq!(snap.high.get(), 7);
        assert_eq!(snap.schema_epoch.get(), 9);
    }

    #[test]
    fn test_oti_field_widths_allow_large_symbol_size() {
        // §3.5.2 requires T/Z/N to represent values >= 65536.
        let oti = Oti {
            f: 1,
            al: 4,
            t: 65_536,
            z: 1,
            n: 1,
        };
        assert_eq!(oti.t, 65_536);
    }

    #[test]
    fn test_budget_product_lattice_semantics() {
        let a = Budget {
            deadline: Some(Duration::from_millis(100)),
            poll_quota: 10,
            cost_quota: Some(500),
            priority: 1,
        };
        let b = Budget {
            deadline: Some(Duration::from_millis(50)),
            poll_quota: 20,
            cost_quota: Some(400),
            priority: 9,
        };
        let c = a.meet(b);
        assert_eq!(c.deadline, Some(Duration::from_millis(50)));
        assert_eq!(c.poll_quota, 10);
        assert_eq!(c.cost_quota, Some(400));
        assert_eq!(c.priority, 9);
    }

    #[test]
    fn test_outcome_ordering_lattice() {
        assert!(Outcome::Ok < Outcome::Err);
        assert!(Outcome::Err < Outcome::Cancelled);
        assert!(Outcome::Cancelled < Outcome::Panicked);
    }

    #[test]
    fn test_witness_key_variants_exhaustive() {
        let pn = PageNumber::new(1).unwrap();

        let a = WitnessKey::Page(pn);
        let b = WitnessKey::Cell {
            btree_root: pn,
            tag: 7,
        };
        let c = WitnessKey::ByteRange {
            page: pn,
            start: 0,
            len: 16,
        };

        assert!(matches!(a, WitnessKey::Page(_)));
        assert!(matches!(b, WitnessKey::Cell { .. }));
        assert!(matches!(c, WitnessKey::ByteRange { .. }));
    }

    #[test]
    fn test_all_glossary_types_derive_debug_clone() {
        fn assert_debug_clone<T: fmt::Debug + Clone>() {}

        assert_debug_clone::<TxnId>();
        assert_debug_clone::<CommitSeq>();
        assert_debug_clone::<TxnEpoch>();
        assert_debug_clone::<TxnToken>();
        assert_debug_clone::<SchemaEpoch>();
        assert_debug_clone::<Snapshot>();
        assert_debug_clone::<VersionPointer>();
        assert_debug_clone::<PageVersion>();
        assert_debug_clone::<ObjectId>();
        assert_debug_clone::<CommitCapsule>();
        assert_debug_clone::<CommitMarker>();
        assert_debug_clone::<Oti>();
        assert_debug_clone::<DecodeProof>();
        assert_debug_clone::<Cx<crate::cx::ComputeCaps>>();
        assert_debug_clone::<Budget>();
        assert_debug_clone::<Outcome>();
        assert_debug_clone::<EpochId>();
        assert_debug_clone::<SymbolValidityWindow>();
        assert_debug_clone::<RemoteCap>();
        assert_debug_clone::<SymbolAuthMasterKeyCap>();
        assert_debug_clone::<IdempotencyKey>();
        assert_debug_clone::<Saga>();
        assert_debug_clone::<Region>();
        assert_debug_clone::<WitnessKey>();
        assert_debug_clone::<RangeKey>();
        assert_debug_clone::<ReadWitness>();
        assert_debug_clone::<WriteWitness>();
        assert_debug_clone::<WitnessIndexSegment>();
        assert_debug_clone::<DependencyEdge>();
        assert_debug_clone::<CommitProof>();
        assert_debug_clone::<TableId>();
        assert_debug_clone::<IndexId>();
        assert_debug_clone::<RowId>();
        assert_debug_clone::<ColumnIdx>();
        assert_debug_clone::<BtreeRef>();
        assert_debug_clone::<SemanticKeyKind>();
        assert_debug_clone::<SemanticKeyRef>();
        assert_debug_clone::<StructuralEffects>();
        assert_debug_clone::<IntentFootprint>();
        assert_debug_clone::<RebaseExpr>();
        assert_debug_clone::<RebaseUnaryOp>();
        assert_debug_clone::<RebaseBinaryOp>();
        assert_debug_clone::<IntentOpKind>();
        assert_debug_clone::<IntentOp>();
        assert_debug_clone::<PageHistory>();
        assert_debug_clone::<ArcCache>();
        assert_debug_clone::<RootManifest>();
        assert_debug_clone::<TxnSlot>();
    }

    fn arb_budget() -> impl Strategy<Value = Budget> {
        (
            prop::option::of(any::<u64>()),
            any::<u32>(),
            prop::option::of(any::<u64>()),
            any::<u8>(),
        )
            .prop_map(|(deadline_ms, poll_quota, cost_quota, priority)| Budget {
                deadline: deadline_ms.map(Duration::from_millis),
                poll_quota,
                cost_quota,
                priority,
            })
    }

    proptest! {
        #[test]
        fn prop_budget_combine_associative(a in arb_budget(), b in arb_budget(), c in arb_budget()) {
            prop_assert_eq!(a.meet(b).meet(c), a.meet(b.meet(c)));
        }

        #[test]
        fn prop_budget_combine_commutative(a in arb_budget(), b in arb_budget()) {
            prop_assert_eq!(a.meet(b), b.meet(a));
        }
    }

    // ── bd-13r.5: RowId + AUTOINCREMENT Semantics ──

    #[test]
    fn test_rowid_reuse_without_autoincrement() {
        let mut alloc = RowIdAllocator::new(RowIdMode::Normal);
        // Table has max rowid 5 → next is 6.
        let r = alloc.allocate(Some(RowId::new(5))).unwrap();
        assert_eq!(r.get(), 6);

        // After deleting row 6, if max existing drops to 3, next is 4 (reuse).
        let r = alloc.allocate(Some(RowId::new(3))).unwrap();
        assert_eq!(r.get(), 4);
    }

    #[test]
    fn test_autoincrement_no_reuse() {
        let mut alloc = RowIdAllocator::new(RowIdMode::AutoIncrement);
        // First allocation, table max is 5.
        let r = alloc.allocate(Some(RowId::new(5))).unwrap();
        assert_eq!(r.get(), 6);

        // After deleting row 6, max existing drops to 3. But AUTOINCREMENT
        // uses high-water mark (6), so next is 7 (no reuse).
        let r = alloc.allocate(Some(RowId::new(3))).unwrap();
        assert_eq!(r.get(), 7);
    }

    #[test]
    fn test_sqlite_sequence_updates() {
        let mut alloc = RowIdAllocator::new(RowIdMode::AutoIncrement);
        assert_eq!(alloc.sequence_high_water(), 0);

        let _ = alloc.allocate(Some(RowId::new(10))).unwrap();
        assert_eq!(alloc.sequence_high_water(), 11);

        // Loading from DB.
        alloc.set_sequence_high_water(100);
        let r = alloc.allocate(Some(RowId::new(50))).unwrap();
        assert_eq!(r.get(), 101);
        assert_eq!(alloc.sequence_high_water(), 101);
    }

    #[test]
    fn test_max_rowid_exhausted_autoincrement() {
        let mut alloc = RowIdAllocator::new(RowIdMode::AutoIncrement);
        // MAX_ROWID reached: AUTOINCREMENT must fail.
        let result = alloc.allocate(Some(RowId::MAX));
        assert!(result.is_err());
    }

    #[test]
    fn test_max_rowid_exhausted_normal() {
        let mut alloc = RowIdAllocator::new(RowIdMode::Normal);
        // MAX_ROWID reached in normal mode: also fails (random probing
        // would happen at the B-tree level, not in the type allocator).
        let result = alloc.allocate(Some(RowId::MAX));
        assert!(result.is_err());
    }

    #[test]
    fn test_rowid_allocate_empty_table() {
        let mut alloc = RowIdAllocator::new(RowIdMode::Normal);
        let r = alloc.allocate(None).unwrap();
        assert_eq!(r.get(), 1);

        let mut alloc = RowIdAllocator::new(RowIdMode::AutoIncrement);
        let r = alloc.allocate(None).unwrap();
        assert_eq!(r.get(), 1);
    }
}
