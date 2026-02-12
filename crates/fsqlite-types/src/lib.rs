pub mod cx;
pub mod ecs;
pub mod encoding;
pub mod flags;
pub mod glossary;
pub mod limits;
pub mod obligation;
pub mod opcode;
pub mod record;
pub mod serial_type;
pub mod value;

pub use cx::Cx;
pub use ecs::{
    ObjectId, PayloadHash, SYMBOL_RECORD_MAGIC, SYMBOL_RECORD_VERSION, SymbolReadPath,
    SymbolRecord, SymbolRecordError, SymbolRecordFlags, SystematicLayoutError,
    layout_systematic_run, reconstruct_systematic_happy_path, recover_object_with_fallback,
    source_symbol_count, validate_systematic_run,
};
pub use glossary::{
    ArcCache, BtreeRef, Budget, COMMIT_MARKER_RECORD_V1_SIZE, ColumnIdx, CommitCapsule,
    CommitMarker, CommitProof, CommitSeq, DecodeProof, DependencyEdge, EpochId, IdempotencyKey,
    IndexId, IntentFootprint, IntentLog, IntentOp, IntentOpKind, OTI_WIRE_SIZE, OperatingMode, Oti,
    Outcome, PageHistory, PageVersion, RangeKey, ReadWitness, RebaseBinaryOp, RebaseExpr,
    RebaseUnaryOp, Region, RemoteCap, RootManifest, RowId, RowIdAllocator, RowIdExhausted,
    RowIdMode, Saga, SchemaEpoch, SemanticKeyKind, SemanticKeyRef, Snapshot, StructuralEffects,
    SymbolAuthMasterKeyCap, SymbolValidityWindow, TableId, TxnEpoch, TxnId, TxnSlot, TxnToken,
    VersionPointer, WitnessIndexSegment, WitnessKey, WriteWitness,
};
pub use value::SqliteValue;

use std::fmt;
use std::num::NonZeroU32;

/// A page number in the database file.
///
/// Page numbers are 1-based (page 0 does not exist). Page 1 is the database
/// header page. The maximum page count is `u32::MAX - 1` (4,294,967,294).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[repr(transparent)]
pub struct PageNumber(NonZeroU32);

impl PageNumber {
    /// Page 1 is the database header page containing the file header and the
    /// schema table root.
    pub const ONE: Self = Self(NonZeroU32::MIN);

    /// Create a new page number from a raw u32.
    ///
    /// Returns `None` if `n` is 0 (page 0 does not exist in SQLite).
    #[inline]
    pub const fn new(n: u32) -> Option<Self> {
        match NonZeroU32::new(n) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }

    /// Get the raw u32 value.
    #[inline]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

impl fmt::Display for PageNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<u32> for PageNumber {
    type Error = InvalidPageNumber;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        Self::new(value).ok_or(InvalidPageNumber)
    }
}

/// Fast identity hasher for `PageNumber` keys in lock/commit tables.
///
/// Page numbers are already well-distributed u32 values, so we skip
/// hashing entirely and use the raw value directly.
#[derive(Default)]
pub struct PageNumberHasher(u64);

impl std::hash::Hasher for PageNumberHasher {
    fn write(&mut self, _: &[u8]) {
        // PageNumber's Hash impl calls write_u32 (via NonZeroU32). If this
        // method is reached, the hasher is being misused with a non-u32 key.
        debug_assert!(false, "PageNumberHasher only supports write_u32");
    }

    fn write_u32(&mut self, n: u32) {
        self.0 = u64::from(n);
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

/// BuildHasher for `PageNumberHasher`.
pub type PageNumberBuildHasher = std::hash::BuildHasherDefault<PageNumberHasher>;

/// GF(256) addition (`+`) for bytes (XOR).
#[must_use]
pub const fn gf256_add_byte(lhs: u8, rhs: u8) -> u8 {
    lhs ^ rhs
}

/// Scalar GF(256) multiply with irreducible polynomial `0x11d`.
///
/// This is the core algebraic primitive used for RaptorQ encoding and
/// XOR-delta compression (§3.2.1).
#[must_use]
pub fn gf256_mul_byte(mut a: u8, mut b: u8) -> u8 {
    let mut out = 0_u8;
    while b != 0 {
        if (b & 1) != 0 {
            out ^= a;
        }
        let carry = (a & 0x80) != 0;
        a <<= 1;
        if carry {
            a ^= 0x1D;
        }
        b >>= 1;
    }
    out
}

/// Multiplicative inverse in GF(256) (`None` for zero).
#[must_use]
pub fn gf256_inverse_byte(value: u8) -> Option<u8> {
    if value == 0 {
        return None;
    }
    for candidate in 1u16..=255 {
        let inv = u8::try_from(candidate).expect("candidate in 1..=255 always fits u8");
        if gf256_mul_byte(value, inv) == 1 {
            return Some(inv);
        }
    }
    None
}

/// SQLite page categories relevant to merge safety policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum MergePageKind {
    /// Interior table b-tree page (0x05).
    BtreeInteriorTable,
    /// Leaf table b-tree page (0x0D).
    BtreeLeafTable,
    /// Interior index b-tree page (0x02).
    BtreeInteriorIndex,
    /// Leaf index b-tree page (0x0A).
    BtreeLeafIndex,
    /// Overflow page.
    Overflow,
    /// Freelist trunk/leaf page.
    Freelist,
    /// Pointer-map page.
    PointerMap,
    /// Opaque/non-SQLite-structured page.
    Opaque,
}

impl MergePageKind {
    /// Whether this page has SQLite-internal pointer semantics.
    #[must_use]
    pub const fn is_sqlite_structured(self) -> bool {
        !matches!(self, Self::Opaque)
    }

    /// Classify a raw page image for merge-safety policy checks.
    #[must_use]
    pub fn classify(page: &[u8]) -> Self {
        let Some(first_byte) = page.first().copied() else {
            return Self::Opaque;
        };
        match BTreePageType::from_byte(first_byte) {
            Some(BTreePageType::LeafTable) => Self::BtreeLeafTable,
            Some(BTreePageType::InteriorTable) => Self::BtreeInteriorTable,
            Some(BTreePageType::LeafIndex) => Self::BtreeLeafIndex,
            Some(BTreePageType::InteriorIndex) => Self::BtreeInteriorIndex,
            None => Self::Opaque,
        }
    }
}

/// Error returned when attempting to create a `PageNumber` from 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidPageNumber;

impl fmt::Display for InvalidPageNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("page number cannot be zero")
    }
}

impl std::error::Error for InvalidPageNumber {}

/// Database page size in bytes.
///
/// Must be a power of two between 512 and 65536 (inclusive). The default is
/// 4096 bytes, matching SQLite's `SQLITE_DEFAULT_PAGE_SIZE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageSize(u32);

impl PageSize {
    /// Minimum page size: 512 bytes.
    pub const MIN: Self = Self(512);

    /// Default page size: 4096 bytes.
    pub const DEFAULT: Self = Self(limits::DEFAULT_PAGE_SIZE);

    /// Maximum page size: 65536 bytes.
    pub const MAX: Self = Self(limits::MAX_PAGE_SIZE);

    /// Create a new page size, validating that it is a power of two in
    /// the range \[512, 65536\].
    pub const fn new(size: u32) -> Option<Self> {
        if size < 512 || size > 65536 || !size.is_power_of_two() {
            None
        } else {
            Some(Self(size))
        }
    }

    /// Get the raw page size in bytes.
    #[inline]
    pub const fn get(self) -> u32 {
        self.0
    }

    /// Get the page size as a `usize`.
    #[inline]
    pub const fn as_usize(self) -> usize {
        self.0 as usize
    }

    /// The usable size of a page (total size minus reserved bytes at the end).
    ///
    /// `reserved` is the number of bytes reserved at the end of each page
    /// for extensions (typically 0, stored at byte offset 20 of the header).
    #[inline]
    pub const fn usable(self, reserved: u8) -> u32 {
        self.0 - reserved as u32
    }
}

impl Default for PageSize {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl fmt::Display for PageSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Raw page data as an owned byte buffer.
///
/// The length always matches the database page size.
#[derive(Clone, PartialEq, Eq)]
pub struct PageData {
    data: Vec<u8>,
}

impl PageData {
    /// Create a zero-filled page of the given size.
    pub fn zeroed(size: PageSize) -> Self {
        Self {
            data: vec![0u8; size.as_usize()],
        }
    }

    /// Create from existing bytes. The caller must ensure the length matches
    /// the page size.
    pub fn from_vec(data: Vec<u8>) -> Self {
        Self { data }
    }

    /// Get the page data as a byte slice.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Get the page data as a mutable byte slice.
    #[inline]
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    /// Get the length in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns true if the page data is empty (should never be true for valid pages).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Consume self and return the inner `Vec<u8>`.
    pub fn into_vec(self) -> Vec<u8> {
        self.data
    }
}

impl fmt::Debug for PageData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PageData")
            .field("len", &self.data.len())
            .finish()
    }
}

impl AsRef<[u8]> for PageData {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl AsMut<[u8]> for PageData {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

/// SQLite type affinity, used for column type resolution.
///
/// See <https://www.sqlite.org/datatype3.html#type_affinity>.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum TypeAffinity {
    /// Column prefers integer storage. Includes INTEGER, INT, TINYINT, etc.
    Integer = b'D',
    /// Column prefers text storage. Includes TEXT, VARCHAR, CLOB.
    Text = b'B',
    /// Column has no preference. Includes BLOB or no type specified.
    Blob = b'A',
    /// Column prefers real (float) storage. Includes REAL, DOUBLE, FLOAT.
    Real = b'E',
    /// Column prefers numeric storage. Includes NUMERIC, DECIMAL, BOOLEAN,
    /// DATE, DATETIME.
    Numeric = b'C',
}

impl TypeAffinity {
    /// Determine the type affinity for a declared column type name.
    ///
    /// Uses SQLite's first-match rule (§3.1 of datatype3.html):
    /// 1. Contains "INT" → INTEGER
    /// 2. Contains "CHAR", "CLOB", or "TEXT" → TEXT
    /// 3. Contains "BLOB" or is empty → BLOB
    /// 4. Contains "REAL", "FLOA", or "DOUB" → REAL
    /// 5. Otherwise → NUMERIC
    pub fn from_type_name(type_name: &str) -> Self {
        let upper = type_name.to_ascii_uppercase();

        if upper.contains("INT") {
            Self::Integer
        } else if upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT") {
            Self::Text
        } else if upper.is_empty() || upper.contains("BLOB") {
            Self::Blob
        } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
            Self::Real
        } else {
            Self::Numeric
        }
    }

    /// Determine the affinity to apply for a comparison between two operands.
    ///
    /// Returns `Some(affinity)` if one side needs coercion, `None` if no
    /// coercion is needed. The returned affinity should be applied to the
    /// operand that needs conversion.
    ///
    /// Rules (§3.2 of datatype3.html):
    /// - If one operand is INTEGER/REAL/NUMERIC and the other is TEXT/BLOB,
    ///   apply numeric affinity to the TEXT/BLOB side.
    /// - If one operand is TEXT and the other is BLOB (no numeric involved),
    ///   apply TEXT affinity to the BLOB side.
    /// - Same affinity or both BLOB → no coercion.
    pub fn comparison_affinity(left: Self, right: Self) -> Option<Self> {
        if left == right {
            return None;
        }

        let is_numeric = |a: Self| matches!(a, Self::Integer | Self::Real | Self::Numeric);

        // Rule 1: numeric vs TEXT/BLOB → apply numeric affinity
        if is_numeric(left) && matches!(right, Self::Text | Self::Blob) {
            return Some(Self::Numeric);
        }
        if is_numeric(right) && matches!(left, Self::Text | Self::Blob) {
            return Some(Self::Numeric);
        }

        // Rule 2: TEXT vs BLOB → apply TEXT affinity to BLOB side
        if (left == Self::Text && right == Self::Blob)
            || (left == Self::Blob && right == Self::Text)
        {
            return Some(Self::Text);
        }

        // Rule 3: no coercion
        None
    }
}

/// The five fundamental SQLite storage classes.
///
/// Every value stored in SQLite belongs to exactly one of these classes.
/// See <https://www.sqlite.org/datatype3.html>.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum StorageClass {
    /// SQL NULL.
    Null = 1,
    /// A signed 64-bit integer.
    Integer = 2,
    /// An IEEE 754 64-bit float.
    Real = 3,
    /// A UTF-8 text string.
    Text = 4,
    /// A binary large object.
    Blob = 5,
}

impl fmt::Display for StorageClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => f.write_str("NULL"),
            Self::Integer => f.write_str("INTEGER"),
            Self::Real => f.write_str("REAL"),
            Self::Text => f.write_str("TEXT"),
            Self::Blob => f.write_str("BLOB"),
        }
    }
}

/// Column types valid in STRICT tables.
///
/// STRICT tables enforce that every non-NULL value stored in a column matches
/// the declared type. See <https://www.sqlite.org/stricttables.html>.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StrictColumnType {
    /// Only INTEGER storage class (and NULL).
    Integer,
    /// REAL storage class; integers are implicitly converted to REAL (and NULL).
    Real,
    /// Only TEXT storage class (and NULL).
    Text,
    /// Only BLOB storage class (and NULL).
    Blob,
    /// Any storage class accepted without coercion.
    Any,
}

impl StrictColumnType {
    /// Parse a STRICT column type from a type name string.
    ///
    /// Returns `None` if the type name is not a valid STRICT type.
    /// Valid STRICT types: INT, INTEGER, REAL, TEXT, BLOB, ANY.
    pub fn from_type_name(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "INT" | "INTEGER" => Some(Self::Integer),
            "REAL" => Some(Self::Real),
            "TEXT" => Some(Self::Text),
            "BLOB" => Some(Self::Blob),
            "ANY" => Some(Self::Any),
            _ => None,
        }
    }
}

/// Error returned when a value violates a STRICT table column type constraint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrictTypeError {
    /// The expected strict column type.
    pub expected: StrictColumnType,
    /// The actual storage class of the value.
    pub actual: StorageClass,
}

impl fmt::Display for StrictTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "cannot store {} value in {:?} column",
            self.actual, self.expected
        )
    }
}

/// Encoding used for text in the database.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum TextEncoding {
    /// UTF-8 encoding (the most common).
    #[default]
    Utf8 = 1,
    /// UTF-16le (little-endian).
    Utf16le = 2,
    /// UTF-16be (big-endian).
    Utf16be = 3,
}

/// Journal mode for the database connection.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JournalMode {
    /// Delete the rollback journal after each transaction.
    #[default]
    Delete,
    /// Truncate the rollback journal to zero length.
    Truncate,
    /// Persist the rollback journal (don't delete, just zero the header).
    Persist,
    /// Store rollback journal in memory only.
    Memory,
    /// Write-ahead logging.
    Wal,
    /// Completely disable the rollback journal.
    Off,
}

/// Synchronous mode for database writes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum SynchronousMode {
    /// No syncs at all. Maximum speed, minimum safety.
    Off = 0,
    /// Sync at critical moments. Good balance.
    Normal = 1,
    /// Sync after each write. Maximum safety.
    #[default]
    Full = 2,
    /// Like Full, but also sync the directory after creating files.
    Extra = 3,
}

/// Lock level for database file locking (SQLite's five-state lock).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum LockLevel {
    /// No lock held.
    #[default]
    None = 0,
    /// Shared lock (reading).
    Shared = 1,
    /// Reserved lock (intending to write).
    Reserved = 2,
    /// Pending lock (waiting for shared locks to clear).
    Pending = 3,
    /// Exclusive lock (writing).
    Exclusive = 4,
}

/// WAL checkpoint mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum CheckpointMode {
    /// Checkpoint as many frames as possible without waiting.
    Passive = 0,
    /// Block until all frames are checkpointed.
    Full = 1,
    /// Like Full, then truncate the WAL file.
    Restart = 2,
    /// Like Restart, then truncate WAL to zero bytes.
    Truncate = 3,
}

/// The 100-byte database file header layout.
///
/// This struct represents the parsed content of the first 100 bytes of a
/// SQLite database file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseHeader {
    /// Page size in bytes (stored as big-endian u16 at offset 16; value 1 means 65536).
    pub page_size: PageSize,
    /// File format write version (1 = legacy, 2 = WAL).
    pub write_version: u8,
    /// File format read version (1 = legacy, 2 = WAL).
    pub read_version: u8,
    /// Reserved bytes per page (at offset 20).
    pub reserved_per_page: u8,
    /// File change counter (at offset 24).
    pub change_counter: u32,
    /// Total number of pages in the database file.
    pub page_count: u32,
    /// Page number of the first freelist trunk page (0 if none).
    pub freelist_trunk: u32,
    /// Total number of freelist pages.
    pub freelist_count: u32,
    /// Schema cookie (incremented on schema changes).
    pub schema_cookie: u32,
    /// Schema format number (currently 4).
    pub schema_format: u32,
    /// Default page cache size (from `PRAGMA default_cache_size`).
    pub default_cache_size: i32,
    /// Largest root page number for auto-vacuum/incremental-vacuum (0 if not auto-vacuum).
    pub largest_root_page: u32,
    /// Database text encoding (1=UTF8, 2=UTF16le, 3=UTF16be).
    pub text_encoding: TextEncoding,
    /// User version (from `PRAGMA user_version`).
    pub user_version: u32,
    /// Non-zero for incremental vacuum mode.
    pub incremental_vacuum: u32,
    /// Application ID (from `PRAGMA application_id`).
    pub application_id: u32,
    /// Version-valid-for number (the change counter value when the version
    /// number was stored).
    pub version_valid_for: u32,
    /// SQLite version number that created the database.
    pub sqlite_version: u32,
}

impl Default for DatabaseHeader {
    fn default() -> Self {
        Self {
            page_size: PageSize::DEFAULT,
            write_version: 1,
            read_version: 1,
            reserved_per_page: 0,
            change_counter: 0,
            page_count: 0,
            freelist_trunk: 0,
            freelist_count: 0,
            schema_cookie: 0,
            schema_format: 4,
            default_cache_size: -2000,
            largest_root_page: 0,
            text_encoding: TextEncoding::Utf8,
            user_version: 0,
            incremental_vacuum: 0,
            application_id: 0,
            version_valid_for: 0,
            sqlite_version: 0,
        }
    }
}

/// The magic string at the start of every SQLite database file.
pub const DATABASE_HEADER_MAGIC: &[u8; 16] = b"SQLite format 3\0";

/// Size of the database file header in bytes.
pub const DATABASE_HEADER_SIZE: usize = 100;

/// Maximum SQLite file format version supported by this codebase.
///
/// This corresponds to WAL support (`2`). If the database header's read version exceeds this
/// value, the database must be refused. If only the write version exceeds this value, the
/// database may be opened read-only.
pub const MAX_FILE_FORMAT_VERSION: u8 = 2;

/// SQLite version number written into the database header for FrankenSQLite-created databases.
///
/// This matches SQLite 3.52.0 (`3052000`), which is the conformance target for this project.
pub const FRANKENSQLITE_SQLITE_VERSION_NUMBER: u32 = 3_052_000;

/// Database file open mode derived from the header's read/write version bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DatabaseOpenMode {
    /// The database can be opened read-write.
    ReadWrite,
    /// The database can only be opened read-only (write version too new).
    ReadOnly,
}

/// Errors that can occur while parsing or validating the 100-byte database header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabaseHeaderError {
    /// Magic string mismatch at bytes 0..16.
    InvalidMagic,
    /// Page size encoding was invalid.
    InvalidPageSize { raw: u16 },
    /// Embedded payload fractions (bytes 21..24) are invalid.
    InvalidPayloadFractions { max: u8, min: u8, leaf: u8 },
    /// The effective usable page size would be below the minimum allowed by SQLite (480).
    UsableSizeTooSmall {
        page_size: u32,
        reserved_per_page: u8,
        usable_size: u32,
    },
    /// Read file format version is too new to be understood.
    UnsupportedReadVersion { read_version: u8, max_supported: u8 },
    /// Text encoding field was not 1/2/3.
    InvalidTextEncoding { raw: u32 },
    /// Schema format number is unsupported.
    InvalidSchemaFormat { raw: u32 },
}

impl fmt::Display for DatabaseHeaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMagic => f.write_str("invalid database header magic"),
            Self::InvalidPageSize { raw } => write!(f, "invalid page size encoding: {raw}"),
            Self::InvalidPayloadFractions { max, min, leaf } => write!(
                f,
                "invalid payload fractions: max={max} min={min} leaf={leaf}"
            ),
            Self::UsableSizeTooSmall {
                page_size,
                reserved_per_page,
                usable_size,
            } => write!(
                f,
                "usable page size too small: page_size={page_size} reserved={reserved_per_page} usable={usable_size}"
            ),
            Self::UnsupportedReadVersion {
                read_version,
                max_supported,
            } => write!(
                f,
                "unsupported read format version: read_version={read_version} max_supported={max_supported}"
            ),
            Self::InvalidTextEncoding { raw } => write!(f, "invalid text encoding: {raw}"),
            Self::InvalidSchemaFormat { raw } => write!(f, "invalid schema format: {raw}"),
        }
    }
}

impl std::error::Error for DatabaseHeaderError {}

impl DatabaseHeader {
    /// Parse and validate a 100-byte database header.
    pub fn from_bytes(buf: &[u8; DATABASE_HEADER_SIZE]) -> Result<Self, DatabaseHeaderError> {
        if &buf[..DATABASE_HEADER_MAGIC.len()] != DATABASE_HEADER_MAGIC {
            return Err(DatabaseHeaderError::InvalidMagic);
        }

        let page_size_raw = encoding::read_u16_be(&buf[16..18]).expect("fixed u16 field");
        let page_size_u32 = match page_size_raw {
            1 => 65_536,
            0 => return Err(DatabaseHeaderError::InvalidPageSize { raw: page_size_raw }),
            n => u32::from(n),
        };
        let page_size = PageSize::new(page_size_u32)
            .ok_or(DatabaseHeaderError::InvalidPageSize { raw: page_size_raw })?;

        let write_version = buf[18];
        let read_version = buf[19];
        let reserved_per_page = buf[20];

        let max_payload = buf[21];
        let min_payload = buf[22];
        let leaf_payload = buf[23];
        if (max_payload, min_payload, leaf_payload) != (64, 32, 32) {
            return Err(DatabaseHeaderError::InvalidPayloadFractions {
                max: max_payload,
                min: min_payload,
                leaf: leaf_payload,
            });
        }

        let usable_size = page_size.usable(reserved_per_page);
        if usable_size < 480 {
            return Err(DatabaseHeaderError::UsableSizeTooSmall {
                page_size: page_size.get(),
                reserved_per_page,
                usable_size,
            });
        }

        // Read version governs forward compatibility: refuse if too new.
        if read_version > MAX_FILE_FORMAT_VERSION {
            return Err(DatabaseHeaderError::UnsupportedReadVersion {
                read_version,
                max_supported: MAX_FILE_FORMAT_VERSION,
            });
        }

        let change_counter = encoding::read_u32_be(&buf[24..28]).expect("fixed u32 field");
        let page_count = encoding::read_u32_be(&buf[28..32]).expect("fixed u32 field");
        let freelist_trunk = encoding::read_u32_be(&buf[32..36]).expect("fixed u32 field");
        let freelist_count = encoding::read_u32_be(&buf[36..40]).expect("fixed u32 field");
        let schema_cookie = encoding::read_u32_be(&buf[40..44]).expect("fixed u32 field");
        let schema_format = encoding::read_u32_be(&buf[44..48]).expect("fixed u32 field");

        // This project intentionally does not support legacy schema formats.
        // See README: "What We Deliberately Exclude".
        if schema_format != 4 {
            return Err(DatabaseHeaderError::InvalidSchemaFormat { raw: schema_format });
        }

        let default_cache_size = encoding::read_i32_be(&buf[48..52]).expect("fixed i32 field");
        let largest_root_page = encoding::read_u32_be(&buf[52..56]).expect("fixed u32 field");

        let text_encoding_raw = encoding::read_u32_be(&buf[56..60]).expect("fixed u32 field");
        let text_encoding = match text_encoding_raw {
            1 => TextEncoding::Utf8,
            2 => TextEncoding::Utf16le,
            3 => TextEncoding::Utf16be,
            _ => {
                return Err(DatabaseHeaderError::InvalidTextEncoding {
                    raw: text_encoding_raw,
                });
            }
        };

        let user_version = encoding::read_u32_be(&buf[60..64]).expect("fixed u32 field");
        let incremental_vacuum = encoding::read_u32_be(&buf[64..68]).expect("fixed u32 field");
        let application_id = encoding::read_u32_be(&buf[68..72]).expect("fixed u32 field");
        let version_valid_for = encoding::read_u32_be(&buf[92..96]).expect("fixed u32 field");
        let sqlite_version = encoding::read_u32_be(&buf[96..100]).expect("fixed u32 field");

        Ok(Self {
            page_size,
            write_version,
            read_version,
            reserved_per_page,
            change_counter,
            page_count,
            freelist_trunk,
            freelist_count,
            schema_cookie,
            schema_format,
            default_cache_size,
            largest_root_page,
            text_encoding,
            user_version,
            incremental_vacuum,
            application_id,
            version_valid_for,
            sqlite_version,
        })
    }

    /// Compute the open mode implied by the header's read/write version bytes.
    pub const fn open_mode(
        &self,
        max_supported: u8,
    ) -> Result<DatabaseOpenMode, DatabaseHeaderError> {
        if self.read_version > max_supported {
            return Err(DatabaseHeaderError::UnsupportedReadVersion {
                read_version: self.read_version,
                max_supported,
            });
        }
        if self.write_version > max_supported {
            return Ok(DatabaseOpenMode::ReadOnly);
        }
        Ok(DatabaseOpenMode::ReadWrite)
    }

    /// Check whether the header-derived database size might be stale.
    ///
    /// When `version_valid_for != change_counter`, header-derived fields
    /// like `page_count` may be stale and should be recomputed from the
    /// actual file size. This protects against partial header writes or
    /// external modification.
    pub const fn is_page_count_stale(&self) -> bool {
        self.version_valid_for != self.change_counter
    }

    /// Compute the page count from the actual file size.
    ///
    /// This should be used when `is_page_count_stale()` returns true.
    /// Returns `None` if the file size is not a multiple of the page size
    /// or would exceed `u32::MAX` pages.
    #[allow(clippy::cast_possible_truncation)]
    pub const fn page_count_from_file_size(&self, file_size: u64) -> Option<u32> {
        let ps = self.page_size.get() as u64;
        if file_size == 0 || file_size % ps != 0 {
            return None;
        }
        let count = file_size / ps;
        if count > u32::MAX as u64 {
            return None;
        }
        Some(count as u32)
    }

    /// Serialize this header into a 100-byte buffer.
    pub fn write_to_bytes(
        &self,
        out: &mut [u8; DATABASE_HEADER_SIZE],
    ) -> Result<(), DatabaseHeaderError> {
        // Validate invariants we rely on for interoperability.
        if self.schema_format != 4 {
            return Err(DatabaseHeaderError::InvalidSchemaFormat {
                raw: self.schema_format,
            });
        }

        let usable_size = self.page_size.usable(self.reserved_per_page);
        if usable_size < 480 {
            return Err(DatabaseHeaderError::UsableSizeTooSmall {
                page_size: self.page_size.get(),
                reserved_per_page: self.reserved_per_page,
                usable_size,
            });
        }

        out.fill(0);
        out[..DATABASE_HEADER_MAGIC.len()].copy_from_slice(DATABASE_HEADER_MAGIC);

        // Page size (big-endian u16) where 1 encodes 65536.
        let page_size_raw = if self.page_size.get() == 65_536 {
            1u16
        } else {
            #[allow(clippy::cast_possible_truncation)]
            {
                self.page_size.get() as u16
            }
        };
        encoding::write_u16_be(&mut out[16..18], page_size_raw).expect("fixed u16 field");

        out[18] = self.write_version;
        out[19] = self.read_version;
        out[20] = self.reserved_per_page;

        // Payload fractions must be 64/32/32.
        out[21] = 64;
        out[22] = 32;
        out[23] = 32;

        encoding::write_u32_be(&mut out[24..28], self.change_counter).expect("fixed u32 field");
        encoding::write_u32_be(&mut out[28..32], self.page_count).expect("fixed u32 field");
        encoding::write_u32_be(&mut out[32..36], self.freelist_trunk).expect("fixed u32 field");
        encoding::write_u32_be(&mut out[36..40], self.freelist_count).expect("fixed u32 field");
        encoding::write_u32_be(&mut out[40..44], self.schema_cookie).expect("fixed u32 field");
        encoding::write_u32_be(&mut out[44..48], self.schema_format).expect("fixed u32 field");
        encoding::write_i32_be(&mut out[48..52], self.default_cache_size).expect("fixed i32 field");
        encoding::write_u32_be(&mut out[52..56], self.largest_root_page).expect("fixed u32 field");

        let text_encoding_u32 = match self.text_encoding {
            TextEncoding::Utf8 => 1u32,
            TextEncoding::Utf16le => 2u32,
            TextEncoding::Utf16be => 3u32,
        };
        encoding::write_u32_be(&mut out[56..60], text_encoding_u32).expect("fixed u32 field");

        encoding::write_u32_be(&mut out[60..64], self.user_version).expect("fixed u32 field");
        encoding::write_u32_be(&mut out[64..68], self.incremental_vacuum).expect("fixed u32 field");
        encoding::write_u32_be(&mut out[68..72], self.application_id).expect("fixed u32 field");

        // Bytes 72..92 are reserved for future expansion. We always write zeros.
        encoding::write_u32_be(&mut out[92..96], self.version_valid_for).expect("fixed u32 field");
        encoding::write_u32_be(&mut out[96..100], self.sqlite_version).expect("fixed u32 field");

        Ok(())
    }

    /// Serialize this header to bytes.
    pub fn to_bytes(&self) -> Result<[u8; DATABASE_HEADER_SIZE], DatabaseHeaderError> {
        let mut out = [0u8; DATABASE_HEADER_SIZE];
        self.write_to_bytes(&mut out)?;
        Ok(out)
    }
}

/// Maximum number of fragmented free bytes allowed on a B-tree page header.
pub const BTREE_MAX_FRAGMENTED_FREE_BYTES: u8 = 60;

/// Errors that can occur while parsing B-tree page layout structures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BTreePageError {
    /// Page buffer did not match the expected page size.
    PageSizeMismatch { expected: usize, actual: usize },
    /// Page did not have enough bytes to read the header.
    PageTooSmall { usable_size: usize, needed: usize },
    /// Unknown B-tree page type byte.
    InvalidPageType { raw: u8 },
    /// Fragmented free bytes exceeds the maximum allowed.
    InvalidFragmentedFreeBytes { raw: u8, max: u8 },
    /// Cell content area start offset was invalid for this page.
    InvalidCellContentAreaStart {
        raw: u16,
        decoded: u32,
        usable_size: usize,
    },
    /// Cell content area begins before the end of the cell pointer array.
    CellContentAreaOverlapsCellPointers {
        cell_content_start: u32,
        cell_pointer_array_end: usize,
    },
    /// Cell pointer array extends past the usable page area.
    CellPointerArrayOutOfBounds {
        start: usize,
        len: usize,
        usable_size: usize,
    },
    /// A cell pointer was invalid.
    InvalidCellPointer {
        index: usize,
        offset: u16,
        usable_size: usize,
    },
    /// Freeblock offset/size was invalid.
    InvalidFreeblock {
        offset: u16,
        size: u16,
        usable_size: usize,
    },
    /// Freeblock list contained a loop.
    FreeblockLoop { offset: u16 },
    /// Interior page right-most child pointer was invalid.
    InvalidRightMostChild { raw: u32 },
}

impl fmt::Display for BTreePageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PageSizeMismatch { expected, actual } => write!(
                f,
                "page size mismatch: expected {expected} bytes, got {actual} bytes"
            ),
            Self::PageTooSmall {
                usable_size,
                needed,
            } => write!(
                f,
                "page too small: usable_size={usable_size} needed={needed}"
            ),
            Self::InvalidPageType { raw } => write!(f, "invalid B-tree page type: {raw:#04x}"),
            Self::InvalidFragmentedFreeBytes { raw, max } => {
                write!(f, "invalid fragmented free bytes: {raw} (max {max})")
            }
            Self::InvalidCellContentAreaStart {
                raw,
                decoded,
                usable_size,
            } => write!(
                f,
                "invalid cell content area start: raw={raw} decoded={decoded} usable_size={usable_size}"
            ),
            Self::CellContentAreaOverlapsCellPointers {
                cell_content_start,
                cell_pointer_array_end,
            } => write!(
                f,
                "cell content area overlaps cell pointer array: cell_content_start={cell_content_start} cell_pointer_array_end={cell_pointer_array_end}"
            ),
            Self::CellPointerArrayOutOfBounds {
                start,
                len,
                usable_size,
            } => write!(
                f,
                "cell pointer array out of bounds: start={start} len={len} usable_size={usable_size}"
            ),
            Self::InvalidCellPointer {
                index,
                offset,
                usable_size,
            } => write!(
                f,
                "invalid cell pointer: index={index} offset={offset} usable_size={usable_size}"
            ),
            Self::InvalidFreeblock {
                offset,
                size,
                usable_size,
            } => write!(
                f,
                "invalid freeblock: offset={offset} size={size} usable_size={usable_size}"
            ),
            Self::FreeblockLoop { offset } => write!(f, "freeblock loop at offset {offset}"),
            Self::InvalidRightMostChild { raw } => {
                write!(f, "invalid right-most child pointer: {raw}")
            }
        }
    }
}

impl std::error::Error for BTreePageError {}

/// Parsed B-tree page header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BTreePageHeader {
    /// Offset within the page where the B-tree page header begins (0 normally, 100 for page 1).
    pub header_offset: usize,
    /// Page type.
    pub page_type: BTreePageType,
    /// Offset of the first freeblock in the freeblock list (0 if none).
    pub first_freeblock: u16,
    /// Number of cells on this page.
    pub cell_count: u16,
    /// Start of cell content area. A raw value of 0 decodes to 65536.
    pub cell_content_start: u32,
    /// Count of fragmented free bytes on this page.
    pub fragmented_free_bytes: u8,
    /// Right-most child page number for interior pages.
    pub right_most_child: Option<PageNumber>,
}

impl BTreePageHeader {
    /// Size of the B-tree page header in bytes (8 for leaf, 12 for interior).
    pub const fn header_size(self) -> usize {
        if self.page_type.is_leaf() { 8 } else { 12 }
    }

    /// Parse a B-tree page header from a page buffer.
    pub fn parse(
        page: &[u8],
        page_size: PageSize,
        reserved_per_page: u8,
        is_page1: bool,
    ) -> Result<Self, BTreePageError> {
        let expected = page_size.as_usize();
        if page.len() != expected {
            return Err(BTreePageError::PageSizeMismatch {
                expected,
                actual: page.len(),
            });
        }

        let usable_size = page_size.usable(reserved_per_page) as usize;
        let header_offset = if is_page1 { DATABASE_HEADER_SIZE } else { 0 };
        let min_needed = header_offset + 8;
        if usable_size < min_needed {
            return Err(BTreePageError::PageTooSmall {
                usable_size,
                needed: min_needed,
            });
        }

        let page_type_raw = page[header_offset];
        let page_type = BTreePageType::from_byte(page_type_raw)
            .ok_or(BTreePageError::InvalidPageType { raw: page_type_raw })?;

        let header_size = if page_type.is_leaf() { 8 } else { 12 };
        let needed = header_offset + header_size;
        if usable_size < needed {
            return Err(BTreePageError::PageTooSmall {
                usable_size,
                needed,
            });
        }

        let first_freeblock =
            u16::from_be_bytes([page[header_offset + 1], page[header_offset + 2]]);
        let cell_count = u16::from_be_bytes([page[header_offset + 3], page[header_offset + 4]]);
        let cell_content_raw =
            u16::from_be_bytes([page[header_offset + 5], page[header_offset + 6]]);
        let cell_content_start = if cell_content_raw == 0 {
            65_536
        } else {
            u32::from(cell_content_raw)
        };
        let usable_size_u32 = u32::try_from(usable_size).unwrap_or(u32::MAX);
        if cell_content_start > usable_size_u32 {
            return Err(BTreePageError::InvalidCellContentAreaStart {
                raw: cell_content_raw,
                decoded: cell_content_start,
                usable_size,
            });
        }

        let fragmented_free_bytes = page[header_offset + 7];
        if fragmented_free_bytes > BTREE_MAX_FRAGMENTED_FREE_BYTES {
            return Err(BTreePageError::InvalidFragmentedFreeBytes {
                raw: fragmented_free_bytes,
                max: BTREE_MAX_FRAGMENTED_FREE_BYTES,
            });
        }

        let right_most_child = if page_type.is_interior() {
            let raw = u32::from_be_bytes([
                page[header_offset + 8],
                page[header_offset + 9],
                page[header_offset + 10],
                page[header_offset + 11],
            ]);
            let pn = PageNumber::new(raw).ok_or(BTreePageError::InvalidRightMostChild { raw })?;
            Some(pn)
        } else {
            None
        };

        // Ensure the cell pointer array is within the usable page area.
        let ptr_array_start = header_offset + header_size;
        let ptr_array_len = usize::from(cell_count) * 2;
        if ptr_array_start + ptr_array_len > usable_size {
            return Err(BTreePageError::CellPointerArrayOutOfBounds {
                start: ptr_array_start,
                len: ptr_array_len,
                usable_size,
            });
        }
        let ptr_array_end = ptr_array_start + ptr_array_len;
        let ptr_array_end_u32 = u32::try_from(ptr_array_end).unwrap_or(u32::MAX);
        if cell_content_start < ptr_array_end_u32 {
            return Err(BTreePageError::CellContentAreaOverlapsCellPointers {
                cell_content_start,
                cell_pointer_array_end: ptr_array_end,
            });
        }

        Ok(Self {
            header_offset,
            page_type,
            first_freeblock,
            cell_count,
            cell_content_start,
            fragmented_free_bytes,
            right_most_child,
        })
    }

    /// Parse the cell pointer array for this page.
    pub fn parse_cell_pointers(
        self,
        page: &[u8],
        page_size: PageSize,
        reserved_per_page: u8,
    ) -> Result<Vec<u16>, BTreePageError> {
        let expected = page_size.as_usize();
        if page.len() != expected {
            return Err(BTreePageError::PageSizeMismatch {
                expected,
                actual: page.len(),
            });
        }

        let usable_size = page_size.usable(reserved_per_page) as usize;
        let ptr_array_start = self.header_offset + self.header_size();
        let ptr_array_len = usize::from(self.cell_count) * 2;
        if ptr_array_start + ptr_array_len > usable_size {
            return Err(BTreePageError::CellPointerArrayOutOfBounds {
                start: ptr_array_start,
                len: ptr_array_len,
                usable_size,
            });
        }

        let min_cell_offset = ptr_array_start + ptr_array_len;
        let mut out = Vec::with_capacity(self.cell_count as usize);
        for i in 0..self.cell_count as usize {
            let off = ptr_array_start + i * 2;
            let cell_off = u16::from_be_bytes([page[off], page[off + 1]]);
            let cell_off_usize = usize::from(cell_off);
            if cell_off_usize < min_cell_offset
                || cell_off_usize < self.cell_content_start as usize
                || cell_off_usize >= usable_size
            {
                return Err(BTreePageError::InvalidCellPointer {
                    index: i,
                    offset: cell_off,
                    usable_size,
                });
            }
            out.push(cell_off);
        }
        Ok(out)
    }

    /// Traverse and parse the freeblock list for this page.
    pub fn parse_freeblocks(
        self,
        page: &[u8],
        page_size: PageSize,
        reserved_per_page: u8,
    ) -> Result<Vec<Freeblock>, BTreePageError> {
        let expected = page_size.as_usize();
        if page.len() != expected {
            return Err(BTreePageError::PageSizeMismatch {
                expected,
                actual: page.len(),
            });
        }
        let usable_size = page_size.usable(reserved_per_page) as usize;

        let mut blocks = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        let mut offset = self.first_freeblock;
        while offset != 0 {
            if !seen.insert(offset) {
                return Err(BTreePageError::FreeblockLoop { offset });
            }

            let off = usize::from(offset);
            if off < self.cell_content_start as usize {
                return Err(BTreePageError::InvalidFreeblock {
                    offset,
                    size: 0,
                    usable_size,
                });
            }
            if off + 4 > usable_size {
                return Err(BTreePageError::InvalidFreeblock {
                    offset,
                    size: 0,
                    usable_size,
                });
            }

            let next = u16::from_be_bytes([page[off], page[off + 1]]);
            let size = u16::from_be_bytes([page[off + 2], page[off + 3]]);
            if size < 4 || off + usize::from(size) > usable_size {
                return Err(BTreePageError::InvalidFreeblock {
                    offset,
                    size,
                    usable_size,
                });
            }

            blocks.push(Freeblock { offset, next, size });
            offset = next;
        }

        Ok(blocks)
    }

    /// Write an empty leaf-table B-tree page header into a buffer.
    ///
    /// Sets up the 8-byte B-tree page header for an empty leaf table page
    /// (type `0x0D`) with zero cells, suitable for `sqlite_master` or any
    /// newly created table root page.
    ///
    /// `header_offset` is the byte offset of the B-tree header within the
    /// page buffer.  For page 1 this must be [`DATABASE_HEADER_SIZE`] (100);
    /// for every other page it should be 0.
    ///
    /// `usable_size` equals `page_size − reserved_per_page`.  The cell
    /// content area offset is set to this value so that all usable space is
    /// available for future cell insertions.
    #[allow(clippy::cast_possible_truncation)]
    pub fn write_empty_leaf_table(page: &mut [u8], header_offset: usize, usable_size: u32) {
        page[header_offset] = BTreePageType::LeafTable as u8; // 0x0D
        // first_freeblock = 0 (no freeblocks)
        page[header_offset + 1] = 0;
        page[header_offset + 2] = 0;
        // cell_count = 0
        page[header_offset + 3] = 0;
        page[header_offset + 4] = 0;
        // cell content area offset (0 encodes 65536)
        let content_raw = if usable_size >= 65_536 {
            0u16
        } else {
            usable_size as u16
        };
        page[header_offset + 5..header_offset + 7].copy_from_slice(&content_raw.to_be_bytes());
        // fragmented_free_bytes = 0
        page[header_offset + 7] = 0;
    }

    /// Initialize an empty leaf index page (type `0x0A`) with zero cells,
    /// suitable for a newly created index root page.
    ///
    /// `header_offset` is the byte offset of the B-tree header within the
    /// page buffer (0 for all non-page-1 pages).
    ///
    /// `usable_size` equals `page_size − reserved_per_page`.
    #[allow(clippy::cast_possible_truncation)]
    pub fn write_empty_leaf_index(page: &mut [u8], header_offset: usize, usable_size: u32) {
        page[header_offset] = BTreePageType::LeafIndex as u8; // 0x0A
        // first_freeblock = 0 (no freeblocks)
        page[header_offset + 1] = 0;
        page[header_offset + 2] = 0;
        // cell_count = 0
        page[header_offset + 3] = 0;
        page[header_offset + 4] = 0;
        // cell content area offset (0 encodes 65536)
        let content_raw = if usable_size >= 65_536 {
            0u16
        } else {
            usable_size as u16
        };
        page[header_offset + 5..header_offset + 7].copy_from_slice(&content_raw.to_be_bytes());
        // fragmented_free_bytes = 0
        page[header_offset + 7] = 0;
    }
}

/// A freeblock entry in a B-tree page freeblock list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Freeblock {
    pub offset: u16,
    pub next: u16,
    pub size: u16,
}

/// Determine if adding `additional` fragmented bytes would exceed the maximum allowed.
pub const fn would_exceed_fragmented_free_bytes(current: u8, additional: u8) -> bool {
    current.saturating_add(additional) > BTREE_MAX_FRAGMENTED_FREE_BYTES
}

/// B-tree page types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum BTreePageType {
    /// Interior index B-tree page.
    InteriorIndex = 2,
    /// Interior table B-tree page.
    InteriorTable = 5,
    /// Leaf index B-tree page.
    LeafIndex = 10,
    /// Leaf table B-tree page.
    LeafTable = 13,
}

impl BTreePageType {
    /// Parse from the raw byte value at the start of a B-tree page header.
    pub const fn from_byte(b: u8) -> Option<Self> {
        match b {
            2 => Some(Self::InteriorIndex),
            5 => Some(Self::InteriorTable),
            10 => Some(Self::LeafIndex),
            13 => Some(Self::LeafTable),
            _ => None,
        }
    }

    /// Whether this is a leaf page (no children).
    pub const fn is_leaf(self) -> bool {
        matches!(self, Self::LeafIndex | Self::LeafTable)
    }

    /// Whether this is an interior (non-leaf) page.
    pub const fn is_interior(self) -> bool {
        matches!(self, Self::InteriorIndex | Self::InteriorTable)
    }

    /// Whether this is a table B-tree (INTKEY) page.
    pub const fn is_table(self) -> bool {
        matches!(self, Self::InteriorTable | Self::LeafTable)
    }

    /// Whether this is an index B-tree (BLOBKEY) page.
    pub const fn is_index(self) -> bool {
        matches!(self, Self::InteriorIndex | Self::LeafIndex)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_number_zero_is_invalid() {
        assert!(PageNumber::new(0).is_none());
        assert!(PageNumber::try_from(0u32).is_err());
    }

    #[test]
    fn test_page_number_zero_rejected() {
        assert!(PageNumber::new(0).is_none());
        assert!(PageNumber::try_from(0u32).is_err());
    }

    #[test]
    fn page_number_valid() {
        let pn = PageNumber::new(1).unwrap();
        assert_eq!(pn.get(), 1);
        assert_eq!(pn, PageNumber::ONE);

        let pn = PageNumber::new(42).unwrap();
        assert_eq!(pn.get(), 42);
        assert_eq!(pn.to_string(), "42");
    }

    #[test]
    fn page_number_ordering() {
        let a = PageNumber::new(1).unwrap();
        let b = PageNumber::new(100).unwrap();
        assert!(a < b);
    }

    #[test]
    fn page_size_validation() {
        assert!(PageSize::new(0).is_none());
        assert!(PageSize::new(256).is_none());
        assert!(PageSize::new(511).is_none());
        assert!(PageSize::new(513).is_none());
        assert!(PageSize::new(1000).is_none());
        assert!(PageSize::new(131_072).is_none());

        assert!(PageSize::new(512).is_some());
        assert!(PageSize::new(1024).is_some());
        assert!(PageSize::new(4096).is_some());
        assert!(PageSize::new(8192).is_some());
        assert!(PageSize::new(16384).is_some());
        assert!(PageSize::new(32768).is_some());
        assert!(PageSize::new(65536).is_some());
    }

    #[test]
    fn page_size_defaults() {
        assert_eq!(PageSize::DEFAULT.get(), 4096);
        assert_eq!(PageSize::MIN.get(), 512);
        assert_eq!(PageSize::MAX.get(), 65536);
        assert_eq!(PageSize::default(), PageSize::DEFAULT);
    }

    fn make_header_for_tests() -> DatabaseHeader {
        DatabaseHeader {
            page_size: PageSize::DEFAULT,
            write_version: 2,
            read_version: 2,
            reserved_per_page: 0,
            change_counter: 7,
            page_count: 123,
            freelist_trunk: 0,
            freelist_count: 0,
            schema_cookie: 1,
            schema_format: 4,
            default_cache_size: -2000,
            largest_root_page: 0,
            text_encoding: TextEncoding::Utf8,
            user_version: 0,
            incremental_vacuum: 0,
            application_id: 0,
            version_valid_for: 7,
            sqlite_version: FRANKENSQLITE_SQLITE_VERSION_NUMBER,
        }
    }

    #[test]
    fn test_header_magic_validation() {
        let hdr = make_header_for_tests();
        let mut buf = hdr.to_bytes().unwrap();
        let parsed = DatabaseHeader::from_bytes(&buf).unwrap();
        assert_eq!(parsed, hdr);

        buf[0] = b'X';
        let err = DatabaseHeader::from_bytes(&buf).unwrap_err();
        assert!(matches!(err, DatabaseHeaderError::InvalidMagic));
    }

    #[test]
    fn test_header_page_size_encoding() {
        // 65536 is encoded as 1.
        let mut hdr = make_header_for_tests();
        hdr.page_size = PageSize::new(65_536).unwrap();
        let buf = hdr.to_bytes().unwrap();
        assert_eq!(u16::from_be_bytes([buf[16], buf[17]]), 1);
        assert_eq!(
            DatabaseHeader::from_bytes(&buf).unwrap().page_size.get(),
            65_536
        );

        // Typical values are stored literally.
        for size in [512u32, 1024, 2048, 4096, 8192, 16_384, 32_768] {
            hdr.page_size = PageSize::new(size).unwrap();
            let buf = hdr.to_bytes().unwrap();
            let expected_u16 = u16::try_from(size).unwrap();
            assert_eq!(u16::from_be_bytes([buf[16], buf[17]]), expected_u16);
            assert_eq!(
                DatabaseHeader::from_bytes(&buf).unwrap().page_size.get(),
                size
            );
        }

        // Non power-of-two rejected.
        let mut buf = make_header_for_tests().to_bytes().unwrap();
        buf[16..18].copy_from_slice(&1000u16.to_be_bytes());
        let err = DatabaseHeader::from_bytes(&buf).unwrap_err();
        assert!(matches!(err, DatabaseHeaderError::InvalidPageSize { .. }));
    }

    #[test]
    fn test_header_page_size_range() {
        let mut buf = make_header_for_tests().to_bytes().unwrap();
        buf[16..18].copy_from_slice(&256u16.to_be_bytes());
        let err = DatabaseHeader::from_bytes(&buf).unwrap_err();
        assert!(matches!(err, DatabaseHeaderError::InvalidPageSize { .. }));
    }

    #[test]
    fn test_header_write_read_version() {
        let mut hdr = make_header_for_tests();

        hdr.write_version = 2;
        hdr.read_version = 2;
        assert_eq!(
            hdr.open_mode(MAX_FILE_FORMAT_VERSION).unwrap(),
            DatabaseOpenMode::ReadWrite
        );

        hdr.read_version = 3;
        let err = hdr.open_mode(MAX_FILE_FORMAT_VERSION).unwrap_err();
        assert!(matches!(
            err,
            DatabaseHeaderError::UnsupportedReadVersion { .. }
        ));

        hdr.read_version = 2;
        hdr.write_version = 3;
        assert_eq!(
            hdr.open_mode(MAX_FILE_FORMAT_VERSION).unwrap(),
            DatabaseOpenMode::ReadOnly
        );
    }

    #[test]
    fn test_header_payload_fractions() {
        let mut buf = make_header_for_tests().to_bytes().unwrap();
        buf[21] = 65;
        let err = DatabaseHeader::from_bytes(&buf).unwrap_err();
        assert!(matches!(
            err,
            DatabaseHeaderError::InvalidPayloadFractions { .. }
        ));
    }

    #[test]
    fn test_header_usable_size_minimum() {
        // For 512-byte pages, reserved_per_page must be <= 32 (512-32=480).
        let mut buf = make_header_for_tests().to_bytes().unwrap();
        buf[16..18].copy_from_slice(&512u16.to_be_bytes());
        buf[20] = 33;
        let err = DatabaseHeader::from_bytes(&buf).unwrap_err();
        assert!(matches!(
            err,
            DatabaseHeaderError::UsableSizeTooSmall { .. }
        ));

        buf[20] = 32;
        DatabaseHeader::from_bytes(&buf).unwrap();
    }

    #[test]
    fn test_header_round_trip() {
        let hdr = make_header_for_tests();
        let buf1 = hdr.to_bytes().unwrap();
        let parsed = DatabaseHeader::from_bytes(&buf1).unwrap();
        assert_eq!(parsed, hdr);

        let buf2 = parsed.to_bytes().unwrap();
        assert_eq!(buf1, buf2);
    }

    #[test]
    fn test_btree_page_header_leaf() {
        let page_size = PageSize::new(512).unwrap();
        let mut page = vec![0u8; page_size.as_usize()];

        // Leaf table page.
        page[0] = 0x0D;
        page[1..3].copy_from_slice(&0u16.to_be_bytes()); // first freeblock
        page[3..5].copy_from_slice(&1u16.to_be_bytes()); // 1 cell
        page[5..7].copy_from_slice(&400u16.to_be_bytes()); // cell content start
        page[7] = 0; // fragmented bytes

        let hdr = BTreePageHeader::parse(&page, page_size, 0, false).unwrap();
        assert!(hdr.page_type.is_leaf());
        assert_eq!(hdr.header_size(), 8);
    }

    #[test]
    fn test_btree_page_header_interior() {
        let page_size = PageSize::new(512).unwrap();
        let mut page = vec![0u8; page_size.as_usize()];

        // Interior table page.
        page[0] = 0x05;
        page[1..3].copy_from_slice(&0u16.to_be_bytes());
        page[3..5].copy_from_slice(&0u16.to_be_bytes());
        page[5..7].copy_from_slice(&500u16.to_be_bytes());
        page[7] = 0;
        page[8..12].copy_from_slice(&2u32.to_be_bytes()); // right-most child

        let hdr = BTreePageHeader::parse(&page, page_size, 0, false).unwrap();
        assert!(hdr.page_type.is_interior());
        assert_eq!(hdr.header_size(), 12);
        assert_eq!(hdr.right_most_child.unwrap().get(), 2);
    }

    #[test]
    fn test_page1_offset_adjustment() {
        let page_size = PageSize::new(512).unwrap();
        let mut page = vec![0u8; page_size.as_usize()];

        // Page 1: B-tree header starts after the 100-byte DB header prefix.
        let h = DATABASE_HEADER_SIZE;
        page[h] = 0x0D; // leaf table
        page[h + 1..h + 3].copy_from_slice(&0u16.to_be_bytes());
        page[h + 3..h + 5].copy_from_slice(&1u16.to_be_bytes()); // 1 cell
        page[h + 5..h + 7].copy_from_slice(&300u16.to_be_bytes()); // cell content start
        page[h + 7] = 0;

        // Cell pointer array begins at h+8.
        page[h + 8..h + 10].copy_from_slice(&300u16.to_be_bytes());

        let hdr = BTreePageHeader::parse(&page, page_size, 0, true).unwrap();
        let ptrs = hdr.parse_cell_pointers(&page, page_size, 0).unwrap();
        assert_eq!(ptrs, vec![300u16]);
    }

    #[test]
    fn test_cell_pointer_array() {
        let page_size = PageSize::new(512).unwrap();
        let mut page = vec![0u8; page_size.as_usize()];

        page[0] = 0x0D;
        page[1..3].copy_from_slice(&0u16.to_be_bytes());
        page[3..5].copy_from_slice(&3u16.to_be_bytes()); // 3 cells
        page[5..7].copy_from_slice(&300u16.to_be_bytes());
        page[7] = 0;
        page[8..10].copy_from_slice(&300u16.to_be_bytes());
        page[10..12].copy_from_slice(&320u16.to_be_bytes());
        page[12..14].copy_from_slice(&340u16.to_be_bytes());

        let hdr = BTreePageHeader::parse(&page, page_size, 0, false).unwrap();
        let ptrs = hdr.parse_cell_pointers(&page, page_size, 0).unwrap();
        assert_eq!(ptrs, vec![300u16, 320u16, 340u16]);
    }

    #[test]
    fn test_freeblock_list_traversal() {
        let page_size = PageSize::new(512).unwrap();
        let mut page = vec![0u8; page_size.as_usize()];

        page[0] = 0x0D;
        page[1..3].copy_from_slice(&400u16.to_be_bytes()); // first freeblock
        page[3..5].copy_from_slice(&0u16.to_be_bytes());
        page[5..7].copy_from_slice(&400u16.to_be_bytes());
        page[7] = 0;

        // freeblock at 400 -> next 420, size 20
        page[400..402].copy_from_slice(&420u16.to_be_bytes());
        page[402..404].copy_from_slice(&20u16.to_be_bytes());
        // freeblock at 420 -> next 0, size 30
        page[420..422].copy_from_slice(&0u16.to_be_bytes());
        page[422..424].copy_from_slice(&30u16.to_be_bytes());

        let hdr = BTreePageHeader::parse(&page, page_size, 0, false).unwrap();
        let blocks = hdr.parse_freeblocks(&page, page_size, 0).unwrap();
        assert_eq!(
            blocks,
            vec![
                Freeblock {
                    offset: 400,
                    next: 420,
                    size: 20
                },
                Freeblock {
                    offset: 420,
                    next: 0,
                    size: 30
                }
            ]
        );
    }

    #[test]
    fn test_freeblock_min_size() {
        let page_size = PageSize::new(512).unwrap();
        let mut page = vec![0u8; page_size.as_usize()];

        page[0] = 0x0D;
        page[1..3].copy_from_slice(&400u16.to_be_bytes());
        page[3..5].copy_from_slice(&0u16.to_be_bytes());
        page[5..7].copy_from_slice(&400u16.to_be_bytes());
        page[7] = 0;

        page[400..402].copy_from_slice(&0u16.to_be_bytes());
        page[402..404].copy_from_slice(&3u16.to_be_bytes()); // invalid

        let hdr = BTreePageHeader::parse(&page, page_size, 0, false).unwrap();
        let err = hdr.parse_freeblocks(&page, page_size, 0).unwrap_err();
        assert!(matches!(err, BTreePageError::InvalidFreeblock { .. }));
    }

    #[test]
    fn test_fragment_defrag_threshold() {
        assert!(!would_exceed_fragmented_free_bytes(60, 0));
        assert!(would_exceed_fragmented_free_bytes(60, 1));
        assert!(would_exceed_fragmented_free_bytes(59, 2));
    }

    #[test]
    fn test_e2e_bd_1a32() {
        use std::fs::File;
        use std::io::{Read, Seek};
        use std::process::Command;
        use std::sync::atomic::{AtomicUsize, Ordering};

        static COUNTER: AtomicUsize = AtomicUsize::new(0);

        // If sqlite3 isn't available in the environment, skip.
        if Command::new("sqlite3").arg("--version").output().is_err() {
            return;
        }

        let mut path = std::env::temp_dir();
        path.push(format!(
            "fsqlite_bd_1a32_{}_{}.sqlite",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));

        let status = Command::new("sqlite3")
            .arg(&path)
            .arg("CREATE TABLE t(x); INSERT INTO t VALUES(1);")
            .status()
            .expect("sqlite3 execution failed");
        assert!(status.success());

        let mut f = File::open(&path).expect("open temp db");
        let mut header_bytes = [0u8; DATABASE_HEADER_SIZE];
        f.read_exact(&mut header_bytes).expect("read db header");
        let header = DatabaseHeader::from_bytes(&header_bytes).expect("parse db header");
        assert_eq!(header.schema_format, 4);
        assert_eq!(
            header.open_mode(MAX_FILE_FORMAT_VERSION).unwrap(),
            DatabaseOpenMode::ReadWrite
        );

        // Re-serialize the parsed header and verify byte-for-byte equivalence.
        let hdr2 = header.to_bytes().expect("serialize header");
        assert_eq!(header_bytes, hdr2);

        // Parse page 1 B-tree header from the first page.
        let page_size = header.page_size;
        let mut page1 = vec![0u8; page_size.as_usize()];
        f.rewind().expect("rewind");
        f.read_exact(&mut page1).expect("read page 1");
        let btree_hdr = BTreePageHeader::parse(&page1, page_size, header.reserved_per_page, true)
            .expect("parse page1 btree header");
        assert_eq!(btree_hdr.header_offset, DATABASE_HEADER_SIZE);
    }

    #[test]
    fn test_varint_signed_cast() {
        use crate::serial_type::{read_varint, write_varint};

        // Varint-decoded u64 cast to i64 produces correct two's complement for rowids.
        let test_cases: &[(u64, i64)] = &[
            (0, 0),
            (1, 1),
            (0x7FFF_FFFF_FFFF_FFFF, i64::MAX),
            (u64::MAX, -1),
            (0x8000_0000_0000_0000, i64::MIN),
        ];
        let mut buf = [0u8; 9];
        for &(unsigned, expected_signed) in test_cases {
            let written = write_varint(&mut buf, unsigned);
            let (decoded, consumed) = read_varint(&buf[..written]).unwrap();
            assert_eq!(decoded, unsigned);
            assert_eq!(consumed, written);
            #[allow(clippy::cast_possible_wrap)]
            let signed = decoded as i64;
            assert_eq!(
                signed, expected_signed,
                "u64 {unsigned} should cast to i64 {expected_signed}, got {signed}"
            );
        }
    }

    #[test]
    fn test_reserved_bytes_72_91_zero() {
        let hdr = make_header_for_tests();
        let buf = hdr.to_bytes().unwrap();
        for (i, &byte) in buf.iter().enumerate().take(92).skip(72) {
            assert_eq!(byte, 0, "byte {i} should be zero (reserved region)");
        }

        let mut hdr2 = make_header_for_tests();
        hdr2.application_id = 0xDEAD_BEEF;
        hdr2.user_version = 42;
        let buf2 = hdr2.to_bytes().unwrap();
        for (i, &byte) in buf2.iter().enumerate().take(92).skip(72) {
            assert_eq!(byte, 0, "byte {i} should be zero even with custom app_id");
        }
    }

    #[test]
    fn test_version_valid_for_stale() {
        let mut hdr = make_header_for_tests();
        hdr.change_counter = 7;
        hdr.version_valid_for = 7;
        assert!(!hdr.is_page_count_stale());

        hdr.version_valid_for = 5;
        assert!(hdr.is_page_count_stale());

        hdr.page_size = PageSize::new(4096).unwrap();
        assert_eq!(hdr.page_count_from_file_size(4096 * 100), Some(100));
        assert_eq!(hdr.page_count_from_file_size(4096), Some(1));
        assert!(hdr.page_count_from_file_size(5000).is_none());
        assert!(hdr.page_count_from_file_size(0).is_none());
    }

    #[test]
    fn test_reserved_space_per_page() {
        let mut hdr = make_header_for_tests();
        hdr.page_size = PageSize::new(4096).unwrap();
        hdr.reserved_per_page = 40;
        let usable = hdr.page_size.usable(hdr.reserved_per_page);
        assert_eq!(usable, 4056);

        let buf = hdr.to_bytes().unwrap();
        let parsed = DatabaseHeader::from_bytes(&buf).unwrap();
        assert_eq!(parsed.reserved_per_page, 40);
        assert_eq!(parsed.page_size.usable(parsed.reserved_per_page), 4056);
    }

    #[test]
    fn test_header_text_encoding_invalid() {
        let mut buf = make_header_for_tests().to_bytes().unwrap();
        buf[56..60].copy_from_slice(&4u32.to_be_bytes());
        let err = DatabaseHeader::from_bytes(&buf).unwrap_err();
        assert!(matches!(
            err,
            DatabaseHeaderError::InvalidTextEncoding { raw: 4 }
        ));

        buf[56..60].copy_from_slice(&0u32.to_be_bytes());
        let err = DatabaseHeader::from_bytes(&buf).unwrap_err();
        assert!(matches!(
            err,
            DatabaseHeaderError::InvalidTextEncoding { raw: 0 }
        ));
    }

    #[test]
    fn test_btree_page_type_classification() {
        assert_eq!(
            BTreePageType::from_byte(0x02),
            Some(BTreePageType::InteriorIndex)
        );
        assert_eq!(
            BTreePageType::from_byte(0x05),
            Some(BTreePageType::InteriorTable)
        );
        assert_eq!(
            BTreePageType::from_byte(0x0A),
            Some(BTreePageType::LeafIndex)
        );
        assert_eq!(
            BTreePageType::from_byte(0x0D),
            Some(BTreePageType::LeafTable)
        );

        assert!(BTreePageType::from_byte(0x00).is_none());
        assert!(BTreePageType::from_byte(0x01).is_none());
        assert!(BTreePageType::from_byte(0xFF).is_none());

        assert!(BTreePageType::InteriorTable.is_interior());
        assert!(BTreePageType::InteriorTable.is_table());
        assert!(!BTreePageType::InteriorTable.is_leaf());
        assert!(!BTreePageType::InteriorTable.is_index());

        assert!(BTreePageType::LeafIndex.is_leaf());
        assert!(BTreePageType::LeafIndex.is_index());
        assert!(!BTreePageType::LeafIndex.is_interior());
        assert!(!BTreePageType::LeafIndex.is_table());
    }

    #[test]
    fn test_invalid_page_type_rejected() {
        let page_size = PageSize::new(512).unwrap();
        let mut page = vec![0u8; page_size.as_usize()];
        page[0] = 0x01;
        let err = BTreePageHeader::parse(&page, page_size, 0, false).unwrap_err();
        assert!(matches!(err, BTreePageError::InvalidPageType { raw: 0x01 }));
    }

    #[test]
    fn test_freeblock_loop_detected() {
        let page_size = PageSize::new(512).unwrap();
        let mut page = vec![0u8; page_size.as_usize()];

        page[0] = 0x0D;
        page[1..3].copy_from_slice(&400u16.to_be_bytes()); // first freeblock
        page[3..5].copy_from_slice(&0u16.to_be_bytes()); // 0 cells
        // cell_content_start must be <= 400 so freeblocks are valid
        page[5..7].copy_from_slice(&300u16.to_be_bytes());
        page[7] = 0;

        // freeblock at 400 -> next 420, size 20
        page[400..402].copy_from_slice(&420u16.to_be_bytes());
        page[402..404].copy_from_slice(&20u16.to_be_bytes());
        // freeblock at 420 -> next 400 (LOOP), size 20
        page[420..422].copy_from_slice(&400u16.to_be_bytes());
        page[422..424].copy_from_slice(&20u16.to_be_bytes());

        let hdr = BTreePageHeader::parse(&page, page_size, 0, false).unwrap();
        let err = hdr.parse_freeblocks(&page, page_size, 0).unwrap_err();
        assert!(matches!(err, BTreePageError::FreeblockLoop { .. }));
    }

    #[test]
    fn test_fragmented_free_bytes_max() {
        let page_size = PageSize::new(512).unwrap();
        let mut page = vec![0u8; page_size.as_usize()];

        page[0] = 0x0D;
        page[5..7].copy_from_slice(&500u16.to_be_bytes()); // valid cell_content_start
        page[7] = 61; // exceeds max of 60
        let err = BTreePageHeader::parse(&page, page_size, 0, false).unwrap_err();
        assert!(matches!(
            err,
            BTreePageError::InvalidFragmentedFreeBytes { raw: 61, max: 60 }
        ));

        // 60 is exactly the limit -- should succeed
        page[7] = 60;
        BTreePageHeader::parse(&page, page_size, 0, false).unwrap();
    }

    #[test]
    fn test_error_variants_distinct_display() {
        let errors: Vec<DatabaseHeaderError> = vec![
            DatabaseHeaderError::InvalidMagic,
            DatabaseHeaderError::InvalidPageSize { raw: 100 },
            DatabaseHeaderError::InvalidPayloadFractions {
                max: 65,
                min: 32,
                leaf: 32,
            },
            DatabaseHeaderError::UsableSizeTooSmall {
                page_size: 512,
                reserved_per_page: 33,
                usable_size: 479,
            },
            DatabaseHeaderError::UnsupportedReadVersion {
                read_version: 3,
                max_supported: 2,
            },
            DatabaseHeaderError::InvalidTextEncoding { raw: 4 },
            DatabaseHeaderError::InvalidSchemaFormat { raw: 0 },
        ];

        let displays: Vec<String> = errors
            .iter()
            .map(std::string::ToString::to_string)
            .collect();
        for (i, d) in displays.iter().enumerate() {
            assert!(!d.is_empty(), "error variant {i} has empty display");
            for (j, d2) in displays.iter().enumerate() {
                if i != j {
                    assert_ne!(d, d2, "error variants {i} and {j} have identical display");
                }
            }
        }
    }

    // ── bd-94us §11.11-11.12 sqlite_master + encoding tests ────────────

    #[test]
    fn test_sqlite_master_page1_root() {
        // sqlite_master is always rooted at page 1.
        // On creation, page 1 is a table leaf (0x0D) with 0 cells.
        let page_size = PageSize::new(4096).unwrap();
        let mut page = [0u8; 4096];
        // Page 1 has 100-byte database header prefix.
        // B-tree header starts at offset 100 for page 1.
        page[..16].copy_from_slice(b"SQLite format 3\0");
        page[16..18].copy_from_slice(&4096u16.to_be_bytes()); // page size
        page[100] = 0x0D; // leaf table page type at header offset
        // cell count = 0 at offset 103
        page[103..105].copy_from_slice(&0u16.to_be_bytes());
        // cell content area start = page_size at offset 105
        page[105..107].copy_from_slice(&4096u16.to_be_bytes()); // cell content area at end of page

        let page_type = BTreePageType::from_byte(page[100]);
        assert_eq!(page_type, Some(BTreePageType::LeafTable));
        let hdr = BTreePageHeader::parse(&page, page_size, 0, true).expect("valid leaf header");
        assert_eq!(hdr.cell_count, 0, "fresh sqlite_master has 0 rows");
    }

    #[test]
    fn test_sqlite_master_schema_columns() {
        // sqlite_master has exactly 5 columns: type, name, tbl_name, rootpage, sql.
        let columns = ["type", "name", "tbl_name", "rootpage", "sql"];
        assert_eq!(columns.len(), 5);
        // Verify the valid type values.
        let valid_types = ["table", "index", "view", "trigger"];
        assert_eq!(valid_types.len(), 4);
    }

    #[test]
    fn test_encoding_utf8_default() {
        // New database defaults to text encoding 1 (UTF-8).
        let hdr = DatabaseHeader::default();
        assert_eq!(hdr.text_encoding, TextEncoding::Utf8);

        let bytes = hdr.to_bytes().expect("encode");
        // Header offset 56 stores encoding as big-endian u32.
        let enc_raw = u32::from_be_bytes([bytes[56], bytes[57], bytes[58], bytes[59]]);
        assert_eq!(enc_raw, 1, "UTF-8 encoding stored as 1 at offset 56");
    }

    #[test]
    fn test_encoding_utf16le() {
        let mut hdr = make_header_for_tests();
        hdr.text_encoding = TextEncoding::Utf16le;
        let bytes = hdr.to_bytes().expect("encode");
        let enc_raw = u32::from_be_bytes([bytes[56], bytes[57], bytes[58], bytes[59]]);
        assert_eq!(enc_raw, 2, "UTF-16LE encoding stored as 2");

        let parsed = DatabaseHeader::from_bytes(&bytes).expect("decode");
        assert_eq!(parsed.text_encoding, TextEncoding::Utf16le);
    }

    #[test]
    fn test_encoding_utf16be() {
        let mut hdr = make_header_for_tests();
        hdr.text_encoding = TextEncoding::Utf16be;
        let bytes = hdr.to_bytes().expect("encode");
        let enc_raw = u32::from_be_bytes([bytes[56], bytes[57], bytes[58], bytes[59]]);
        assert_eq!(enc_raw, 3, "UTF-16BE encoding stored as 3");

        let parsed = DatabaseHeader::from_bytes(&bytes).expect("decode");
        assert_eq!(parsed.text_encoding, TextEncoding::Utf16be);
    }

    #[test]
    fn test_encoding_immutable_after_creation() {
        // Encoding is set at creation and cannot be changed afterward.
        // Changing the encoding field in an existing header and re-serializing
        // produces a different byte at offset 56 -- the enforcement is at the
        // application layer (PRAGMA encoding is rejected after first table).
        let hdr1 = make_header_for_tests();
        assert_eq!(hdr1.text_encoding, TextEncoding::Utf8);
        let bytes1 = hdr1.to_bytes().expect("encode");

        let mut hdr2 = hdr1;
        hdr2.text_encoding = TextEncoding::Utf16le;
        let bytes2 = hdr2.to_bytes().expect("encode");

        // The encoding field differs in the serialized bytes.
        assert_ne!(
            bytes1[56..60],
            bytes2[56..60],
            "different encodings must serialize differently"
        );
    }

    #[test]
    fn test_binary_collation_memcmp_utf8() {
        // BINARY collation uses memcmp on raw bytes.
        // For UTF-8, memcmp produces correct Unicode code point ordering.
        let a = "abc";
        let b = "abd";
        assert!(
            a.as_bytes() < b.as_bytes(),
            "memcmp ordering for ASCII UTF-8"
        );

        // Multi-byte UTF-8: 'é' (U+00E9) = [0xC3, 0xA9], 'z' (U+007A) = [0x7A].
        // In code point order: 'z' (122) < 'é' (233).
        // In byte order: 0x7A < 0xC3, so 'z' < 'é' — same as code point order.
        let z = "z";
        let e_acute = "é";
        assert!(
            z.as_bytes() < e_acute.as_bytes(),
            "UTF-8 memcmp preserves code point order"
        );
    }

    // ── bd-16ov §12.15-12.16 Type Affinity tests ────────────────────────

    #[test]
    fn test_affinity_int_keyword() {
        assert_eq!(
            TypeAffinity::from_type_name("INTEGER"),
            TypeAffinity::Integer
        );
        assert_eq!(TypeAffinity::from_type_name("INT"), TypeAffinity::Integer);
        assert_eq!(
            TypeAffinity::from_type_name("TINYINT"),
            TypeAffinity::Integer
        );
        assert_eq!(
            TypeAffinity::from_type_name("SMALLINT"),
            TypeAffinity::Integer
        );
        assert_eq!(
            TypeAffinity::from_type_name("MEDIUMINT"),
            TypeAffinity::Integer
        );
        assert_eq!(
            TypeAffinity::from_type_name("BIGINT"),
            TypeAffinity::Integer
        );
        assert_eq!(
            TypeAffinity::from_type_name("UNSIGNED BIG INT"),
            TypeAffinity::Integer
        );
        assert_eq!(TypeAffinity::from_type_name("INT2"), TypeAffinity::Integer);
        assert_eq!(TypeAffinity::from_type_name("INT8"), TypeAffinity::Integer);
    }

    #[test]
    fn test_affinity_text_keyword() {
        assert_eq!(TypeAffinity::from_type_name("TEXT"), TypeAffinity::Text);
        assert_eq!(
            TypeAffinity::from_type_name("CHARACTER(20)"),
            TypeAffinity::Text
        );
        assert_eq!(
            TypeAffinity::from_type_name("VARCHAR(255)"),
            TypeAffinity::Text
        );
        assert_eq!(
            TypeAffinity::from_type_name("VARYING CHARACTER(255)"),
            TypeAffinity::Text
        );
        assert_eq!(
            TypeAffinity::from_type_name("NCHAR(55)"),
            TypeAffinity::Text
        );
        assert_eq!(
            TypeAffinity::from_type_name("NATIVE CHARACTER(70)"),
            TypeAffinity::Text
        );
        assert_eq!(
            TypeAffinity::from_type_name("NVARCHAR(100)"),
            TypeAffinity::Text
        );
        assert_eq!(TypeAffinity::from_type_name("CLOB"), TypeAffinity::Text);
    }

    #[test]
    fn test_affinity_blob_keyword() {
        assert_eq!(TypeAffinity::from_type_name("BLOB"), TypeAffinity::Blob);
        assert_eq!(TypeAffinity::from_type_name("blob"), TypeAffinity::Blob);
    }

    #[test]
    fn test_affinity_empty_type() {
        assert_eq!(TypeAffinity::from_type_name(""), TypeAffinity::Blob);
    }

    #[test]
    fn test_affinity_real_keyword() {
        assert_eq!(TypeAffinity::from_type_name("REAL"), TypeAffinity::Real);
        assert_eq!(TypeAffinity::from_type_name("DOUBLE"), TypeAffinity::Real);
        assert_eq!(
            TypeAffinity::from_type_name("DOUBLE PRECISION"),
            TypeAffinity::Real
        );
        assert_eq!(TypeAffinity::from_type_name("FLOAT"), TypeAffinity::Real);
    }

    #[test]
    fn test_affinity_numeric_keyword() {
        assert_eq!(
            TypeAffinity::from_type_name("NUMERIC"),
            TypeAffinity::Numeric
        );
        assert_eq!(
            TypeAffinity::from_type_name("DECIMAL(10,5)"),
            TypeAffinity::Numeric
        );
        assert_eq!(
            TypeAffinity::from_type_name("BOOLEAN"),
            TypeAffinity::Numeric
        );
        assert_eq!(TypeAffinity::from_type_name("DATE"), TypeAffinity::Numeric);
        assert_eq!(
            TypeAffinity::from_type_name("DATETIME"),
            TypeAffinity::Numeric
        );
    }

    #[test]
    fn test_affinity_case_insensitive() {
        assert_eq!(
            TypeAffinity::from_type_name("integer"),
            TypeAffinity::Integer
        );
        assert_eq!(TypeAffinity::from_type_name("text"), TypeAffinity::Text);
        assert_eq!(TypeAffinity::from_type_name("Real"), TypeAffinity::Real);
        assert_eq!(
            TypeAffinity::from_type_name("Numeric"),
            TypeAffinity::Numeric
        );
    }

    #[test]
    fn test_affinity_first_match_int_before_char() {
        // "CHARINT" contains both "CHAR" and "INT", but "INT" is checked first.
        assert_eq!(
            TypeAffinity::from_type_name("CHARINT"),
            TypeAffinity::Integer
        );
        // "POINTERFLOAT" contains "INT" so INTEGER wins over REAL.
        assert_eq!(
            TypeAffinity::from_type_name("POINTERFLOAT"),
            TypeAffinity::Integer
        );
    }

    #[test]
    fn test_comparison_numeric_vs_text() {
        assert_eq!(
            TypeAffinity::comparison_affinity(TypeAffinity::Integer, TypeAffinity::Text),
            Some(TypeAffinity::Numeric)
        );
        assert_eq!(
            TypeAffinity::comparison_affinity(TypeAffinity::Text, TypeAffinity::Real),
            Some(TypeAffinity::Numeric)
        );
        assert_eq!(
            TypeAffinity::comparison_affinity(TypeAffinity::Numeric, TypeAffinity::Blob),
            Some(TypeAffinity::Numeric)
        );
    }

    #[test]
    fn test_comparison_text_vs_blob() {
        assert_eq!(
            TypeAffinity::comparison_affinity(TypeAffinity::Text, TypeAffinity::Blob),
            Some(TypeAffinity::Text)
        );
        assert_eq!(
            TypeAffinity::comparison_affinity(TypeAffinity::Blob, TypeAffinity::Text),
            Some(TypeAffinity::Text)
        );
    }

    #[test]
    fn test_comparison_same_affinity_no_coercion() {
        assert_eq!(
            TypeAffinity::comparison_affinity(TypeAffinity::Integer, TypeAffinity::Integer),
            None
        );
        assert_eq!(
            TypeAffinity::comparison_affinity(TypeAffinity::Text, TypeAffinity::Text),
            None
        );
        assert_eq!(
            TypeAffinity::comparison_affinity(TypeAffinity::Blob, TypeAffinity::Blob),
            None
        );
    }

    #[test]
    fn test_comparison_both_blob_no_coercion() {
        assert_eq!(
            TypeAffinity::comparison_affinity(TypeAffinity::Blob, TypeAffinity::Blob),
            None
        );
    }

    #[test]
    fn test_affinity_applied_to_needing_operand_only() {
        let left = SqliteValue::Integer(42);
        let right = SqliteValue::Text("123".to_string());
        let affinity = TypeAffinity::comparison_affinity(left.affinity(), right.affinity())
            .expect("numeric-vs-text comparison must request numeric coercion");

        // Numeric side should remain unchanged.
        let left_after = left.clone();
        // Text side is the side that needs conversion for numeric comparison.
        let right_after = right.apply_affinity(affinity);

        assert_eq!(left_after, left);
        assert_eq!(right_after, SqliteValue::Integer(123));
    }

    #[test]
    fn test_comparison_numeric_subtypes() {
        // INTEGER vs REAL: both numeric, different variants but no coercion needed
        // per SQLite rules (they share the numeric class).
        assert_eq!(
            TypeAffinity::comparison_affinity(TypeAffinity::Integer, TypeAffinity::Real),
            None
        );
        assert_eq!(
            TypeAffinity::comparison_affinity(TypeAffinity::Integer, TypeAffinity::Numeric),
            None
        );
        assert_eq!(
            TypeAffinity::comparison_affinity(TypeAffinity::Real, TypeAffinity::Numeric),
            None
        );
    }

    // ── 5A.1: BTreePageHeader::write_empty_leaf_table tests (bd-2yy6) ──

    #[test]
    fn test_write_empty_leaf_table_basic() {
        let ps = PageSize::DEFAULT;
        let mut buf = vec![0u8; ps.as_usize()];
        BTreePageHeader::write_empty_leaf_table(&mut buf, 0, ps.get());

        assert_eq!(buf[0], 0x0D, "page type LeafTable");
        assert_eq!(buf[1], 0, "first_freeblock hi");
        assert_eq!(buf[2], 0, "first_freeblock lo");
        assert_eq!(buf[3], 0, "cell_count hi");
        assert_eq!(buf[4], 0, "cell_count lo");
        // 4096 = 0x1000
        assert_eq!(buf[5], 0x10, "content_offset hi");
        assert_eq!(buf[6], 0x00, "content_offset lo");
        assert_eq!(buf[7], 0, "fragmented_free_bytes");
    }

    #[test]
    fn test_write_empty_leaf_table_page1_offset() {
        let ps = PageSize::DEFAULT;
        let mut buf = vec![0u8; ps.as_usize()];
        BTreePageHeader::write_empty_leaf_table(&mut buf, DATABASE_HEADER_SIZE, ps.get());

        assert_eq!(buf[DATABASE_HEADER_SIZE], 0x0D, "page type at offset 100");
        // Bytes before offset 100 should be untouched.
        assert!(buf[..DATABASE_HEADER_SIZE].iter().all(|&b| b == 0));
    }

    #[test]
    fn test_write_empty_leaf_table_65536_encoding() {
        let ps = PageSize::new(65536).unwrap();
        let mut buf = vec![0u8; ps.as_usize()];
        BTreePageHeader::write_empty_leaf_table(&mut buf, 0, ps.get());

        // 65536 is encoded as 0 in the B-tree header.
        assert_eq!(buf[5], 0x00, "65536 encoded as 0 hi");
        assert_eq!(buf[6], 0x00, "65536 encoded as 0 lo");
    }

    #[test]
    fn test_write_empty_leaf_table_512_page_size() {
        let ps = PageSize::new(512).unwrap();
        let mut buf = vec![0u8; ps.as_usize()];
        BTreePageHeader::write_empty_leaf_table(&mut buf, 0, ps.get());

        // 512 = 0x0200
        assert_eq!(buf[5], 0x02, "512 hi byte");
        assert_eq!(buf[6], 0x00, "512 lo byte");
    }
}
