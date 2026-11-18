use bitflags::bitflags;

/// Canonical big-endian encoding for on-disk SQLite-compatible structures.
pub trait BigEndianEncode {
    type Bytes;

    fn to_be_canonical(self) -> Self::Bytes;
}

/// Canonical big-endian decoding for on-disk SQLite-compatible structures.
pub trait BigEndianDecode: Sized {
    type Bytes;

    fn from_be_canonical(bytes: Self::Bytes) -> Self;
}

/// Canonical little-endian encoding for FrankenSQLite-native ECS structures.
pub trait LittleEndianEncode {
    type Bytes;

    fn to_le_canonical(self) -> Self::Bytes;
}

/// Canonical little-endian decoding for FrankenSQLite-native ECS structures.
pub trait LittleEndianDecode: Sized {
    type Bytes;

    fn from_le_canonical(bytes: Self::Bytes) -> Self;
}

macro_rules! impl_endian_codecs {
    ($ty:ty, $len:expr) => {
        impl BigEndianEncode for $ty {
            type Bytes = [u8; $len];

            fn to_be_canonical(self) -> Self::Bytes {
                self.to_be_bytes()
            }
        }

        impl BigEndianDecode for $ty {
            type Bytes = [u8; $len];

            fn from_be_canonical(bytes: Self::Bytes) -> Self {
                Self::from_be_bytes(bytes)
            }
        }

        impl LittleEndianEncode for $ty {
            type Bytes = [u8; $len];

            fn to_le_canonical(self) -> Self::Bytes {
                self.to_le_bytes()
            }
        }

        impl LittleEndianDecode for $ty {
            type Bytes = [u8; $len];

            fn from_le_canonical(bytes: Self::Bytes) -> Self {
                Self::from_le_bytes(bytes)
            }
        }
    };
}

impl_endian_codecs!(u16, 2);
impl_endian_codecs!(u32, 4);
impl_endian_codecs!(u64, 8);
impl_endian_codecs!(i32, 4);

fn read_array<const N: usize>(input: &[u8], offset: usize) -> Option<[u8; N]> {
    let end = offset.checked_add(N)?;
    input.get(offset..end)?.try_into().ok()
}

fn write_array<const N: usize>(output: &mut [u8], offset: usize, bytes: [u8; N]) -> bool {
    let Some(end) = offset.checked_add(N) else {
        return false;
    };
    let Some(dst) = output.get_mut(offset..end) else {
        return false;
    };
    dst.copy_from_slice(&bytes);
    true
}

#[must_use]
pub fn read_u16_be(input: &[u8], offset: usize) -> Option<u16> {
    read_array::<2>(input, offset).map(u16::from_be_canonical)
}

#[must_use]
pub fn read_u32_be(input: &[u8], offset: usize) -> Option<u32> {
    read_array::<4>(input, offset).map(u32::from_be_canonical)
}

#[must_use]
pub fn read_u64_be(input: &[u8], offset: usize) -> Option<u64> {
    read_array::<8>(input, offset).map(u64::from_be_canonical)
}

#[must_use]
pub fn read_i32_be(input: &[u8], offset: usize) -> Option<i32> {
    read_array::<4>(input, offset).map(i32::from_be_canonical)
}

#[must_use]
pub fn read_u16_le(input: &[u8], offset: usize) -> Option<u16> {
    read_array::<2>(input, offset).map(u16::from_le_canonical)
}

#[must_use]
pub fn read_u32_le(input: &[u8], offset: usize) -> Option<u32> {
    read_array::<4>(input, offset).map(u32::from_le_canonical)
}

#[must_use]
pub fn read_u64_le(input: &[u8], offset: usize) -> Option<u64> {
    read_array::<8>(input, offset).map(u64::from_le_canonical)
}

#[must_use]
pub fn write_u16_be(output: &mut [u8], offset: usize, value: u16) -> bool {
    write_array(output, offset, value.to_be_canonical())
}

#[must_use]
pub fn write_u32_be(output: &mut [u8], offset: usize, value: u32) -> bool {
    write_array(output, offset, value.to_be_canonical())
}

#[must_use]
pub fn write_u64_be(output: &mut [u8], offset: usize, value: u64) -> bool {
    write_array(output, offset, value.to_be_canonical())
}

#[must_use]
pub fn write_i32_be(output: &mut [u8], offset: usize, value: i32) -> bool {
    write_array(output, offset, value.to_be_canonical())
}

#[must_use]
pub fn write_u16_le(output: &mut [u8], offset: usize, value: u16) -> bool {
    write_array(output, offset, value.to_le_canonical())
}

#[must_use]
pub fn write_u32_le(output: &mut [u8], offset: usize, value: u32) -> bool {
    write_array(output, offset, value.to_le_canonical())
}

#[must_use]
pub fn write_u64_le(output: &mut [u8], offset: usize, value: u64) -> bool {
    write_array(output, offset, value.to_le_canonical())
}

bitflags! {
    /// Flags for `sqlite3_open_v2()`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct OpenFlags: u32 {
        /// Open for reading only.
        const READONLY       = 0x0000_0001;
        /// Open for reading and writing.
        const READWRITE      = 0x0000_0002;
        /// Create the database if it doesn't exist.
        const CREATE         = 0x0000_0004;
        /// Interpret the filename as a URI.
        const URI            = 0x0000_0040;
        /// Open an in-memory database.
        const MEMORY         = 0x0000_0080;
        /// Use the main database only (no attached databases).
        const NOMUTEX        = 0x0000_8000;
        /// Use the serialized threading mode.
        const FULLMUTEX      = 0x0001_0000;
        /// Use shared cache mode.
        const SHAREDCACHE    = 0x0002_0000;
        /// Use private cache mode.
        const PRIVATECACHE   = 0x0004_0000;
        /// Do not follow symlinks.
        const NOFOLLOW       = 0x0100_0000;
    }
}

impl Default for OpenFlags {
    fn default() -> Self {
        Self::READWRITE | Self::CREATE
    }
}

bitflags! {
    /// Flags for VFS file sync operations.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct SyncFlags: u8 {
        /// Normal sync (SQLITE_SYNC_NORMAL).
        const NORMAL   = 0x02;
        /// Full sync (SQLITE_SYNC_FULL).
        const FULL     = 0x03;
        /// Sync the data only, not the directory (SQLITE_SYNC_DATAONLY).
        const DATAONLY = 0x10;
    }
}

bitflags! {
    /// Flags for VFS file open operations.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct VfsOpenFlags: u32 {
        /// Main database file.
        const MAIN_DB          = 0x0000_0100;
        /// Main journal file.
        const MAIN_JOURNAL     = 0x0000_0800;
        /// Temporary database file.
        const TEMP_DB          = 0x0000_0200;
        /// Temporary journal file.
        const TEMP_JOURNAL     = 0x0000_1000;
        /// Sub-journal file.
        const SUBJOURNAL       = 0x0000_2000;
        /// Super-journal file (formerly master journal).
        const SUPER_JOURNAL    = 0x0000_4000;
        /// WAL file.
        const WAL              = 0x0008_0000;
        /// Open for exclusive access.
        const EXCLUSIVE        = 0x0000_0010;
        /// Create the file if it doesn't exist.
        const CREATE           = 0x0000_0004;
        /// Open for reading and writing.
        const READWRITE        = 0x0000_0002;
        /// Open for reading only.
        const READONLY         = 0x0000_0001;
        /// Delete on close.
        const DELETEONCLOSE    = 0x0000_0008;
        /// FrankenSQLite only, no SQLite counterpart: the file takes no locks
        /// and maps no `-shm`, for a read-only `immutable=1` open (bd-j2dtf).
        /// Every lock request succeeds without touching the file, every
        /// write-lock request is refused, and shared memory is unavailable.
        const NO_LOCK          = 0x0400_0000;
    }
}

bitflags! {
    /// Flags for VFS access checks.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct AccessFlags: u8 {
        /// Check if the file exists.
        const EXISTS    = 0;
        /// Check if the file is readable and writable.
        const READWRITE = 1;
        /// Check if the file is readable.
        const READ      = 2;
    }
}

bitflags! {
    /// Prepare statement flags.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct PrepareFlags: u32 {
        /// The statement is persistent (will be reused).
        const PERSISTENT = 0x01;
        /// The statement may be normalized.
        const NORMALIZE  = 0x02;
        /// Do not scan the schema before preparing.
        const NO_VTAB    = 0x04;
    }
}

bitflags! {
    /// Internal flags on the Mem/sqlite3_value structure.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct MemFlags: u16 {
        /// Value is NULL.
        const NULL      = 0x0001;
        /// Value is a string.
        const STR       = 0x0002;
        /// Value is an integer.
        const INT       = 0x0004;
        /// Value is a real (float).
        const REAL      = 0x0008;
        /// Value is a BLOB.
        const BLOB      = 0x0010;
        /// Value is an integer that should be treated as real (optimization).
        const INT_REAL  = 0x0020;
        /// Auxiliary data attached.
        const AFF_MASK  = 0x003F;
        /// Memory needs to be freed.
        const DYN       = 0x0040;
        /// Value is a static string (no free needed).
        const STATIC    = 0x0080;
        /// Value is stored in an ephemeral buffer.
        const EPHEM     = 0x0100;
        /// Value has been cleared/invalidated.
        const CLEARED   = 0x0200;
        /// String has a NUL terminator.
        const TERM      = 0x0400;
        /// Has a subtype value.
        const SUBTYPE   = 0x0800;
        /// Zero-filled blob.
        const ZERO      = 0x1000;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::{Read, Seek};
    use std::process::Command;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn open_flags_default() {
        let flags = OpenFlags::default();
        assert!(flags.contains(OpenFlags::READWRITE));
        assert!(flags.contains(OpenFlags::CREATE));
        assert!(!flags.contains(OpenFlags::READONLY));
    }

    #[test]
    fn open_flags_combinations() {
        let flags = OpenFlags::READONLY | OpenFlags::URI;
        assert!(flags.contains(OpenFlags::READONLY));
        assert!(flags.contains(OpenFlags::URI));
        assert!(!flags.contains(OpenFlags::CREATE));
    }

    #[test]
    fn sync_flags() {
        let flags = SyncFlags::FULL | SyncFlags::DATAONLY;
        assert!(flags.contains(SyncFlags::FULL));
        assert!(flags.contains(SyncFlags::DATAONLY));
    }

    #[test]
    fn vfs_open_flags() {
        let flags = VfsOpenFlags::MAIN_DB | VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE;
        assert!(flags.contains(VfsOpenFlags::MAIN_DB));
        assert!(flags.contains(VfsOpenFlags::CREATE));
    }

    #[test]
    fn prepare_flags() {
        let flags = PrepareFlags::PERSISTENT | PrepareFlags::NORMALIZE;
        assert!(flags.contains(PrepareFlags::PERSISTENT));
        assert!(flags.contains(PrepareFlags::NORMALIZE));
    }

    #[test]
    fn mem_flags() {
        let flags = MemFlags::INT | MemFlags::STATIC;
        assert!(flags.contains(MemFlags::INT));
        assert!(flags.contains(MemFlags::STATIC));
        assert!(!flags.contains(MemFlags::NULL));
    }

    #[test]
    fn test_sqlite_structures_big_endian() {
        let header = crate::DatabaseHeader {
            page_size: crate::PageSize::new(4096).expect("valid page size"),
            change_counter: 0x0102_0304,
            page_count: 0x0A0B_0C0D,
            default_cache_size: -2000,
            ..crate::DatabaseHeader::default()
        };

        let bytes = header
            .to_bytes()
            .expect("header serialization must succeed");
        assert_eq!(read_u16_be(&bytes, 16), Some(4096));
        assert_eq!(read_u32_be(&bytes, 24), Some(header.change_counter));
        assert_eq!(read_u32_be(&bytes, 28), Some(header.page_count));
        assert_eq!(read_i32_be(&bytes, 48), Some(header.default_cache_size));

        let mut page = vec![0u8; header.page_size.as_usize()];
        page[0] = crate::BTreePageType::LeafTable as u8;
        assert!(write_u16_be(&mut page, 1, 0));
        assert!(write_u16_be(&mut page, 3, 1));
        assert!(write_u16_be(&mut page, 5, 400));
        page[7] = 0;

        let parsed = crate::BTreePageHeader::parse(&page, header.page_size, 0, false)
            .expect("btree header parsing must succeed");
        assert_eq!(parsed.cell_count, 1);
        assert_eq!(read_u16_be(&page, 3), Some(parsed.cell_count));
        assert_eq!(read_u16_be(&page, 5), Some(400));
    }

    #[test]
    fn test_mixed_endian_udp_documented() {
        // Intentional protocol split: network header uses big-endian while
        // payload scalar fields use little-endian.
        let mut packet = [0u8; 12];
        assert!(write_u16_be(&mut packet, 0, 0xBEEF));
        assert!(write_u16_be(&mut packet, 2, 8)); // payload bytes
        assert!(write_u64_le(&mut packet, 4, 0x1122_3344_5566_7788));

        assert_eq!(read_u16_be(&packet, 0), Some(0xBEEF));
        assert_eq!(read_u16_be(&packet, 2), Some(8));
        assert_eq!(read_u64_le(&packet, 4), Some(0x1122_3344_5566_7788));
    }

    #[test]
    fn test_e2e_canonical_bytes_match_sqlite_where_required() {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);

        if Command::new("sqlite3").arg("--version").output().is_err() {
            return;
        }

        let mut path = std::env::temp_dir();
        path.push(format!(
            "fsqlite_bd_22n_7_{}_{}.sqlite",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));

        let status = Command::new("sqlite3")
            .arg(&path)
            .arg("CREATE TABLE t(x); INSERT INTO t VALUES(1);")
            .status()
            .expect("sqlite3 execution must succeed");
        assert!(status.success(), "sqlite3 command failed");

        let mut file = File::open(&path).expect("must open sqlite file");
        let mut header_bytes = [0u8; crate::DATABASE_HEADER_SIZE];
        file.read_exact(&mut header_bytes)
            .expect("must read sqlite header");

        let parsed =
            crate::DatabaseHeader::from_bytes(&header_bytes).expect("header parse must succeed");
        let rewritten = parsed.to_bytes().expect("header encode must succeed");
        assert_eq!(header_bytes, rewritten, "canonical bytes must roundtrip");
        assert_eq!(
            parsed
                .open_mode(crate::MAX_FILE_FORMAT_VERSION)
                .expect("open mode derivation must succeed"),
            crate::DatabaseOpenMode::ReadWrite
        );

        let encoded_page_size = if parsed.page_size.get() == 65_536 {
            1
        } else {
            u16::try_from(parsed.page_size.get()).expect("page size <= u16")
        };
        assert_eq!(read_u16_be(&header_bytes, 16), Some(encoded_page_size));
        assert_eq!(read_u32_be(&header_bytes, 24), Some(parsed.change_counter));
        assert_eq!(read_u32_be(&header_bytes, 28), Some(parsed.page_count));

        let mut page1 = vec![0u8; parsed.page_size.as_usize()];
        file.rewind().expect("rewind to file start");
        file.read_exact(&mut page1).expect("read page 1");
        let btree =
            crate::BTreePageHeader::parse(&page1, parsed.page_size, parsed.reserved_per_page, true)
                .expect("parse page1 btree header");

        assert_eq!(page1[crate::DATABASE_HEADER_SIZE], btree.page_type as u8);
        assert_eq!(
            read_u16_be(&page1, crate::DATABASE_HEADER_SIZE + 3),
            Some(btree.cell_count)
        );
    }
}
