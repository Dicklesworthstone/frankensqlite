//! Connection open flags, analogous to `rusqlite::OpenFlags`.

use fsqlite_error::FrankenError;
use fsqlite_types::flags::VfsOpenFlags;

use crate::Connection;

/// Subset of SQLite open flags that cass uses, mirroring `rusqlite::OpenFlags`.
///
/// Under the hood these map to `VfsOpenFlags`.
#[derive(Debug, Clone, Copy)]
pub struct OpenFlags(u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpenDisposition {
    ReadOnly,
    WriteExisting,
    WriteCreate,
}

impl OpenFlags {
    /// Open the database in read-only mode.
    pub const SQLITE_OPEN_READ_ONLY: Self = Self(0x01);

    /// Open the database for reading and writing.
    pub const SQLITE_OPEN_READ_WRITE: Self = Self(0x02);

    /// Create the database if it does not exist (combined with READ_WRITE).
    pub const SQLITE_OPEN_CREATE: Self = Self(0x04);

    /// Interpret the database path as a URI.
    ///
    /// The compat layer accepts this flag for `sqlite3_open_v2` parity even
    /// though URI query-parameter semantics are not implemented yet.
    pub const SQLITE_OPEN_URI: Self = Self(0x40);

    /// Request that the connection omit per-connection mutexes.
    ///
    /// FrankenSQLite's compat layer does not model SQLite's connection mutex
    /// configuration directly, so this is accepted and ignored.
    pub const SQLITE_OPEN_NO_MUTEX: Self = Self(0x0000_8000);

    /// Request that the connection use full mutex protection.
    ///
    /// FrankenSQLite's compat layer does not model SQLite's connection mutex
    /// configuration directly, so this is accepted and ignored.
    pub const SQLITE_OPEN_FULL_MUTEX: Self = Self(0x0001_0000);

    /// Request shared-cache participation.
    ///
    /// FrankenSQLite does not expose SQLite's shared-cache subsystem, but it
    /// accepts the flag so callers can pass through stock `sqlite3_open_v2`
    /// masks without being rejected in the compat layer.
    pub const SQLITE_OPEN_SHARED_CACHE: Self = Self(0x0002_0000);

    /// Request a private page cache.
    ///
    /// FrankenSQLite does not expose SQLite's shared-cache subsystem, but it
    /// accepts the flag so callers can pass through stock `sqlite3_open_v2`
    /// masks without being rejected in the compat layer.
    pub const SQLITE_OPEN_PRIVATE_CACHE: Self = Self(0x0004_0000);

    /// Request extended result codes from the connection.
    ///
    /// FrankenSQLite already returns rich Rust error variants, so this flag is
    /// accepted and ignored for API compatibility.
    pub const SQLITE_OPEN_EXRESCODE: Self = Self(0x0200_0000);

    const ACCESS_MODE_MASK: u32 =
        Self::SQLITE_OPEN_READ_ONLY.0 | Self::SQLITE_OPEN_READ_WRITE.0 | Self::SQLITE_OPEN_CREATE.0;
    const ACCEPTED_ANCILLARY_MASK: u32 = Self::SQLITE_OPEN_URI.0
        | Self::SQLITE_OPEN_NO_MUTEX.0
        | Self::SQLITE_OPEN_FULL_MUTEX.0
        | Self::SQLITE_OPEN_SHARED_CACHE.0
        | Self::SQLITE_OPEN_PRIVATE_CACHE.0
        | Self::SQLITE_OPEN_EXRESCODE.0;
    const SUPPORTED_MASK: u32 = Self::ACCESS_MODE_MASK | Self::ACCEPTED_ANCILLARY_MASK;

    /// Default flags: READ_WRITE | CREATE.
    pub fn default_flags() -> Self {
        Self(Self::SQLITE_OPEN_READ_WRITE.0 | Self::SQLITE_OPEN_CREATE.0)
    }

    /// Combine two flag sets with bitwise OR.
    pub fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Check if a flag is set.
    pub fn contains(self, flag: Self) -> bool {
        self.0 & flag.0 == flag.0
    }

    /// Convert to `VfsOpenFlags`.
    pub fn to_vfs_flags(self) -> VfsOpenFlags {
        let mut flags = VfsOpenFlags::MAIN_DB;
        if self.contains(Self::SQLITE_OPEN_READ_ONLY) {
            flags |= VfsOpenFlags::READONLY;
        } else if self.contains(Self::SQLITE_OPEN_READ_WRITE) {
            flags |= VfsOpenFlags::READWRITE;
        }
        if self.contains(Self::SQLITE_OPEN_CREATE) {
            flags |= VfsOpenFlags::CREATE;
        }
        flags
    }
}

fn classify_access_mode(flags: OpenFlags) -> Result<OpenDisposition, FrankenError> {
    validate_open_flags(flags)?;

    let access_mode = flags.0 & OpenFlags::ACCESS_MODE_MASK;
    let read_only = access_mode & OpenFlags::SQLITE_OPEN_READ_ONLY.0 != 0;
    let read_write = access_mode & OpenFlags::SQLITE_OPEN_READ_WRITE.0 != 0;
    let create = access_mode & OpenFlags::SQLITE_OPEN_CREATE.0 != 0;

    match (read_only, read_write, create) {
        (true, false, false) => Ok(OpenDisposition::ReadOnly),
        (false, true, false) => Ok(OpenDisposition::WriteExisting),
        (false, true, true) => Ok(OpenDisposition::WriteCreate),
        _ => Err(FrankenError::TypeMismatch {
            expected:
                "one of SQLITE_OPEN_READ_ONLY, SQLITE_OPEN_READ_WRITE, or SQLITE_OPEN_READ_WRITE | SQLITE_OPEN_CREATE"
                    .into(),
            actual: format!("open flags 0x{:x}", flags.0),
        }),
    }
}

fn validate_open_flags(flags: OpenFlags) -> Result<(), FrankenError> {
    let unsupported_bits = flags.0 & !OpenFlags::SUPPORTED_MASK;
    if unsupported_bits != 0 {
        return Err(FrankenError::TypeMismatch {
            expected: "SQLite-compatible open flags supported by fsqlite::compat::OpenFlags".into(),
            actual: format!(
                "unsupported open flag bits 0x{unsupported_bits:x} in 0x{:x}",
                flags.0
            ),
        });
    }

    let mutex_mode_bits =
        flags.0 & (OpenFlags::SQLITE_OPEN_NO_MUTEX.0 | OpenFlags::SQLITE_OPEN_FULL_MUTEX.0);
    if mutex_mode_bits == (OpenFlags::SQLITE_OPEN_NO_MUTEX.0 | OpenFlags::SQLITE_OPEN_FULL_MUTEX.0)
    {
        return Err(FrankenError::TypeMismatch {
            expected: "at most one of SQLITE_OPEN_NO_MUTEX or SQLITE_OPEN_FULL_MUTEX".into(),
            actual: format!("open flags 0x{:x}", flags.0),
        });
    }

    let cache_mode_bits =
        flags.0 & (OpenFlags::SQLITE_OPEN_SHARED_CACHE.0 | OpenFlags::SQLITE_OPEN_PRIVATE_CACHE.0);
    if cache_mode_bits
        == (OpenFlags::SQLITE_OPEN_SHARED_CACHE.0 | OpenFlags::SQLITE_OPEN_PRIVATE_CACHE.0)
    {
        return Err(FrankenError::TypeMismatch {
            expected: "at most one of SQLITE_OPEN_SHARED_CACHE or SQLITE_OPEN_PRIVATE_CACHE".into(),
            actual: format!("open flags 0x{:x}", flags.0),
        });
    }

    Ok(())
}

fn open_read_only_connection(path: &str) -> Result<Connection, FrankenError> {
    if path == ":memory:" {
        return Err(FrankenError::NotImplemented(
            "read-only :memory: connections are not supported".to_owned(),
        ));
    }
    Connection::open_schema_only(path)
}

impl std::ops::BitOr for OpenFlags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        self.union(rhs)
    }
}

/// Open a connection with the given flags.
///
/// When `SQLITE_OPEN_READ_ONLY` is set, the connection is opened in
/// schema-only mode: table/index/view/trigger definitions are loaded
/// but no row data is read into the in-memory `MemDatabase`. Queries
/// are served through pager-backed B-tree cursors, which read directly
/// from the on-disk pages. This makes opening even multi-gigabyte
/// databases near-instantaneous.
///
/// # Examples
///
/// ```ignore
/// use fsqlite::compat::{OpenFlags, open_with_flags};
///
/// let conn = open_with_flags("my.db", OpenFlags::SQLITE_OPEN_READ_ONLY)?;
/// ```
pub fn open_with_flags(path: &str, flags: OpenFlags) -> Result<Connection, FrankenError> {
    match classify_access_mode(flags)? {
        OpenDisposition::ReadOnly => open_read_only_connection(path),
        OpenDisposition::WriteExisting => {
            if path == ":memory:" {
                Connection::open(path)
            } else {
                Connection::open_existing(path)
            }
        }
        OpenDisposition::WriteCreate => Connection::open(path),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_flags_contain_rw_and_create() {
        let flags = OpenFlags::default_flags();
        assert!(flags.contains(OpenFlags::SQLITE_OPEN_READ_WRITE));
        assert!(flags.contains(OpenFlags::SQLITE_OPEN_CREATE));
    }

    #[test]
    fn bitor_combines_flags() {
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE;
        assert!(flags.contains(OpenFlags::SQLITE_OPEN_READ_WRITE));
        assert!(flags.contains(OpenFlags::SQLITE_OPEN_CREATE));
    }

    #[test]
    fn open_with_flags_in_memory() {
        let conn = open_with_flags(":memory:", OpenFlags::default_flags()).unwrap();
        assert_eq!(conn.path(), ":memory:");
    }

    #[test]
    fn vfs_flags_conversion() {
        let flags = OpenFlags::default_flags();
        let vfs = flags.to_vfs_flags();
        assert!(vfs.contains(VfsOpenFlags::READWRITE));
        assert!(vfs.contains(VfsOpenFlags::CREATE));
        assert!(vfs.contains(VfsOpenFlags::MAIN_DB));
    }

    #[test]
    fn vfs_flags_conversion_preserves_read_only() {
        let vfs = OpenFlags::SQLITE_OPEN_READ_ONLY.to_vfs_flags();
        assert!(vfs.contains(VfsOpenFlags::READONLY));
        assert!(!vfs.contains(VfsOpenFlags::READWRITE));
        assert!(vfs.contains(VfsOpenFlags::MAIN_DB));
    }

    #[test]
    fn vfs_flags_conversion_prefers_read_only_when_both_are_present() {
        let vfs =
            (OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_READ_WRITE).to_vfs_flags();
        assert!(vfs.contains(VfsOpenFlags::READONLY));
        assert!(!vfs.contains(VfsOpenFlags::READWRITE));
    }

    #[test]
    fn open_with_flags_read_write_without_create_missing_db_fails() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("missing.db");
        let error = open_with_flags(path.to_str().unwrap(), OpenFlags::SQLITE_OPEN_READ_WRITE)
            .expect_err("READ_WRITE without CREATE should not create a missing database");
        assert!(matches!(error, FrankenError::CannotOpen { .. }));
        assert!(!path.exists());
    }

    #[test]
    fn classify_access_mode_rejects_create_without_read_write() {
        let error = classify_access_mode(OpenFlags::SQLITE_OPEN_CREATE)
            .expect_err("CREATE alone is not a valid sqlite3_open_v2 access mode");
        assert!(matches!(error, FrankenError::TypeMismatch { .. }));
    }

    #[test]
    fn classify_access_mode_rejects_read_only_create_combo() {
        let error =
            classify_access_mode(OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_CREATE)
                .expect_err("READ_ONLY | CREATE is not a valid sqlite3_open_v2 access mode");
        assert!(matches!(error, FrankenError::TypeMismatch { .. }));
    }

    #[test]
    fn classify_access_mode_accepts_common_sqlite_ancillary_flags() {
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_PRIVATE_CACHE
            | OpenFlags::SQLITE_OPEN_EXRESCODE;

        let mode = classify_access_mode(flags).expect(
            "common sqlite3_open_v2 ancillary flags should not be rejected by the compat layer",
        );
        assert_eq!(mode, OpenDisposition::WriteCreate);
    }

    #[test]
    fn classify_access_mode_rejects_conflicting_mutex_flags() {
        let error = classify_access_mode(
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_FULL_MUTEX,
        )
        .expect_err("conflicting mutex flags should be rejected explicitly");
        assert!(matches!(error, FrankenError::TypeMismatch { .. }));
    }

    #[test]
    fn classify_access_mode_rejects_conflicting_cache_flags() {
        let error = classify_access_mode(
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_SHARED_CACHE
                | OpenFlags::SQLITE_OPEN_PRIVATE_CACHE,
        )
        .expect_err("conflicting cache-mode flags should be rejected explicitly");
        assert!(matches!(error, FrankenError::TypeMismatch { .. }));
    }

    #[test]
    fn open_with_flags_read_only_in_memory_is_rejected() {
        let error = open_with_flags(":memory:", OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect_err("compat open must not return a writable connection for READ_ONLY");
        assert!(matches!(error, FrankenError::NotImplemented(_)));
    }

    #[test]
    fn open_with_flags_accepts_common_sqlite_ancillary_flags() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("ancillary_flags.db");
        let conn = open_with_flags(
            path.to_str().unwrap(),
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_PRIVATE_CACHE
                | OpenFlags::SQLITE_OPEN_EXRESCODE,
        )
        .expect("ancillary sqlite3_open_v2 flags should be accepted by the compat layer");
        conn.execute("CREATE TABLE t(x INTEGER)").unwrap();
        assert!(path.exists());
    }
}
