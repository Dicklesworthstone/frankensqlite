pub mod cx;
pub mod flags;
pub mod limits;
pub mod opcode;
pub mod record;
pub mod serial_type;
pub mod value;
pub mod cx;

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
    pub const ONE: Self = Self(match NonZeroU32::new(1) {
        Some(v) => v,
        None => unreachable!(),
    });

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

/// A transaction ID for MVCC versioning.
///
/// Transaction IDs are monotonically increasing 64-bit integers. `TxnId::ZERO`
/// represents data that was in the database file before any MVCC transaction
/// (the "on-disk" version).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[repr(transparent)]
pub struct TxnId(u64);

impl TxnId {
    /// The zero transaction ID, representing pre-existing on-disk data.
    pub const ZERO: Self = Self(0);

    /// Create a new transaction ID from a raw u64.
    #[inline]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Get the raw u64 value.
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Get the next transaction ID (wrapping on overflow, which is practically
    /// impossible with u64).
    #[inline]
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.wrapping_add(1))
    }
}

impl fmt::Display for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "txn#{}", self.0)
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
}
