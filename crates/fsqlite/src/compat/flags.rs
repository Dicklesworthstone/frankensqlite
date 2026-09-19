//! Connection open flags, analogous to `rusqlite::OpenFlags`.

use std::borrow::Cow;

use fsqlite_error::FrankenError;
use fsqlite_types::flags::VfsOpenFlags;

use crate::{Connection, ConnectionEnv};

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
    /// Recognizes `file:` filenames, percent escapes, local authorities,
    /// fragments, and `mode=ro|rw|rwc|memory`. Access modes may narrow, never
    /// expand, the flags supplied by the caller. Unsupported VFS/locking or
    /// shared-cache requests are rejected before opening a connection.
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

/// Fully resolved before any filesystem or connection side effect. Keeping the
/// path and disposition together prevents a decoded read-only URI from taking
/// the original caller's READWRITE|CREATE branch.
#[derive(Debug)]
struct ResolvedOpen<'a> {
    path: Cow<'a, str>,
    disposition: OpenDisposition,
}

fn uri_error(detail: impl Into<String>) -> FrankenError {
    FrankenError::FunctionError(detail.into())
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Decode once, AFTER splitting the URI's raw delimiters. Unlike form-encoded
/// data, '+' is a literal filename character. Malformed % escapes remain
/// literal, as in SQLite; NUL and non-UTF-8 results are refused for this str API.
fn decode_uri_component(raw: &str) -> Result<Cow<'_, str>, FrankenError> {
    if raw.as_bytes().contains(&0) {
        return Err(uri_error("NUL byte in database URI"));
    }
    if !raw.as_bytes().contains(&b'%') {
        return Ok(Cow::Borrowed(raw));
    }
    let input = raw.as_bytes();
    let mut decoded = Vec::new();
    decoded
        .try_reserve_exact(input.len())
        .map_err(|_| FrankenError::OutOfMemory)?;
    let mut index = 0;
    while index < input.len() {
        if input[index] == b'%'
            && let Some((high, low)) = input
                .get(index + 1)
                .and_then(|byte| hex_digit(*byte))
                .zip(input.get(index + 2).and_then(|byte| hex_digit(*byte)))
        {
            let byte = (high << 4) | low;
            if byte == 0 {
                return Err(uri_error("NUL byte in database URI"));
            }
            decoded.push(byte);
            index += 3;
        } else {
            decoded.push(input[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded)
        .map(Cow::Owned)
        .map_err(|_| uri_error("database URI is not valid UTF-8 after percent decoding"))
}

fn narrow_uri_mode(
    current: OpenDisposition,
    requested: OpenDisposition,
    value: &str,
) -> Result<OpenDisposition, FrankenError> {
    let allowed = match current {
        OpenDisposition::ReadOnly => requested == OpenDisposition::ReadOnly,
        OpenDisposition::WriteExisting => requested != OpenDisposition::WriteCreate,
        OpenDisposition::WriteCreate => true,
    };
    if allowed {
        Ok(requested)
    } else {
        Err(uri_error(format!("access mode not allowed: {value}")))
    }
}

/// Only values explicitly requesting no change may use an unsupported boolean
/// VFS option. Do not silently accept a locking/immutability promise we cannot
/// enforce. In particular, mapping immutable=1 to an ordinary writable open is
/// wrong, and disabling native locks would undermine concurrent writers.
fn disabled_uri_boolean(value: &str) -> bool {
    value.is_empty()
        || value.bytes().all(|byte| byte == b'0')
        || ["false", "no", "off"]
            .iter()
            .any(|word| value.eq_ignore_ascii_case(word))
}

fn resolve_uri_query(
    query: &str,
    mut disposition: OpenDisposition,
    mut shared_cache: bool,
) -> Result<(OpenDisposition, bool), FrankenError> {
    let mut memory = false;
    for parameter in query.split('&').filter(|parameter| !parameter.is_empty()) {
        let (key, value) = parameter.split_once('=').unwrap_or((parameter, ""));
        let key = decode_uri_component(key)?;
        let value = decode_uri_component(value)?;
        match key.as_ref() {
            "mode" => {
                let requested = match value.as_ref() {
                    "ro" => Some(OpenDisposition::ReadOnly),
                    "rw" => Some(OpenDisposition::WriteExisting),
                    "rwc" => Some(OpenDisposition::WriteCreate),
                    "memory" => None,
                    _ => return Err(uri_error(format!("no such access mode: {value}"))),
                };
                if let Some(requested) = requested {
                    disposition = narrow_uri_mode(disposition, requested, &value)?;
                    memory = false;
                } else {
                    // Preserve the caller's access ceiling across repeated
                    // mode parameters; memory must not reset it to writable.
                    memory = true;
                }
            }
            "cache" => {
                shared_cache = match value.as_ref() {
                    "private" => false,
                    "shared" => true,
                    _ => return Err(uri_error(format!("no such cache mode: {value}"))),
                };
            }
            "immutable" | "nolock" if disabled_uri_boolean(&value) => {}
            "immutable" | "nolock" | "vfs" | "modeof" | "psow" => {
                return Err(FrankenError::NotImplemented(format!(
                    "database URI option {key}={value} is not supported"
                )));
            }
            // Unknown options are ignored, not appended to the filename.
            _ => {}
        }
    }
    if shared_cache {
        return Err(FrankenError::NotImplemented(
            "shared-cache database URIs are not supported; use cache=private".to_owned(),
        ));
    }
    Ok((disposition, memory))
}

fn resolve_open(path: &str, flags: OpenFlags) -> Result<ResolvedOpen<'_>, FrankenError> {
    let disposition = classify_access_mode(flags)?;
    if path.as_bytes().contains(&0) {
        return Err(uri_error("NUL byte in database filename"));
    }
    let Some(mut uri) = path
        .strip_prefix("file:")
        .filter(|_| flags.contains(OpenFlags::SQLITE_OPEN_URI))
    else {
        return Ok(ResolvedOpen {
            path: Cow::Borrowed(path),
            disposition,
        });
    };

    // Authority is not percent-decoded. Only the empty authority and literal
    // localhost are supported, not remote hosts or alternate UNC namespaces.
    if let Some(authority_and_path) = uri.strip_prefix("//") {
        let end = authority_and_path
            .find('/')
            .unwrap_or(authority_and_path.len());
        let authority = &authority_and_path[..end];
        if !authority.is_empty() && authority != "localhost" {
            return Err(uri_error(format!("invalid uri authority: {authority}")));
        }
        uri = &authority_and_path[end..];
    }
    let without_fragment = uri.split_once('#').map_or(uri, |(head, _)| head);
    let (raw_path, query) = without_fragment
        .split_once('?')
        .unwrap_or((without_fragment, ""));
    let mut filename = decode_uri_component(raw_path)?;
    let (disposition, memory) = resolve_uri_query(
        query,
        disposition,
        flags.contains(OpenFlags::SQLITE_OPEN_SHARED_CACHE),
    )?;
    if memory {
        filename = Cow::Borrowed(":memory:");
    } else if filename.is_empty() {
        return Err(FrankenError::NotImplemented(
            "temporary-file database URIs are not supported; use mode=memory".to_owned(),
        ));
    }
    if filename == ":memory:" && disposition == OpenDisposition::ReadOnly {
        return Err(FrankenError::NotImplemented(
            "read-only :memory: connections are not supported".to_owned(),
        ));
    }
    #[cfg(windows)]
    {
        let bytes = filename.as_bytes();
        if bytes.len() >= 4
            && bytes[0] == b'/'
            && bytes[1].is_ascii_alphabetic()
            && bytes[2] == b':'
            && bytes[3] == b'/'
        {
            filename = Cow::Owned(filename[1..].to_owned());
        }
    }
    Ok(ResolvedOpen {
        path: filename,
        disposition,
    })
}

async fn open_read_only_connection_with_env(
    path: &str,
    env: ConnectionEnv,
) -> Result<Connection, FrankenError> {
    if path == ":memory:" {
        return Err(FrankenError::NotImplemented(
            "read-only :memory: connections are not supported".to_owned(),
        ));
    }
    Connection::open_schema_only_with_env(path, env).await
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
/// With `SQLITE_OPEN_URI`, local `file:` URIs are resolved before selecting
/// this access mode. `mode=ro` never takes a writable path, `mode=rw` never
/// creates a missing file, and private `mode=memory` never uses disk. Percent
/// escapes, query delimiters, literal '+', and fragments follow SQLite URI
/// conventions. Ordinary filenames and calls without URI enabled are unchanged.
///
/// Shared-cache URIs, temporary-file URIs, alternate VFS selection, permission
/// copying, powersafe-overwrite overrides, and enabled immutable/nolock options
/// return explicit errors rather than pretending to honor unsupported behavior.
/// NUL/non-UTF-8 decoded names and attempts to widen an access mode (including
/// across a repeated `mode=memory`) are refused. URI handling here applies to
/// this open, not subsequent SQL ATTACH statements or direct core constructors.
///
/// # Examples
///
/// ```ignore
/// use fsqlite::compat::{OpenFlags, open_with_flags};
///
/// let conn = open_with_flags("my.db", OpenFlags::SQLITE_OPEN_READ_ONLY)?;
/// ```
pub async fn open_with_flags(path: &str, flags: OpenFlags) -> Result<Connection, FrankenError> {
    open_with_flags_with_env(path, flags, ConnectionEnv::default()).await
}

// `pub`, not `pub(crate)`: the enclosing `flags` module is private, so `pub`
// already limits this to the crate; `pub(crate)` here trips
// `clippy::redundant_pub_crate` and breaks the workspace `-D warnings` gate.
pub async fn open_with_flags_with_env(
    path: &str,
    flags: OpenFlags,
    env: ConnectionEnv,
) -> Result<Connection, FrankenError> {
    let resolved = resolve_open(path, flags)?;
    let path = resolved.path.as_ref();
    match resolved.disposition {
        OpenDisposition::ReadOnly => open_read_only_connection_with_env(path, env).await,
        OpenDisposition::WriteExisting => {
            if path == ":memory:" {
                Connection::open_with_env(path, env).await
            } else {
                Connection::open_existing_with_env(path, env).await
            }
        }
        OpenDisposition::WriteCreate => Connection::open_with_env(path, env).await,
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
        asupersync::test_utils::run_test(|| async {
            let conn = open_with_flags(":memory:", OpenFlags::default_flags())
                .await
                .unwrap();
            assert_eq!(conn.path(), ":memory:");
        });
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
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::TempDir::new().unwrap();
            let path = dir.path().join("missing.db");
            let error = open_with_flags(path.to_str().unwrap(), OpenFlags::SQLITE_OPEN_READ_WRITE)
                .await
                .expect_err("READ_WRITE without CREATE should not create a missing database");
            assert!(matches!(error, FrankenError::CannotOpen { .. }));
            assert!(!path.exists());
        });
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
        asupersync::test_utils::run_test(|| async {
            let error = open_with_flags(":memory:", OpenFlags::SQLITE_OPEN_READ_ONLY)
                .await
                .expect_err("compat open must not return a writable connection for READ_ONLY");
            assert!(matches!(error, FrankenError::NotImplemented(_)));
        });
    }

    #[test]
    fn open_with_flags_accepts_common_sqlite_ancillary_flags() {
        asupersync::test_utils::run_test(|| async {
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
            .await
            .expect("ancillary sqlite3_open_v2 flags should be accepted by the compat layer");
            conn.execute("CREATE TABLE t(x INTEGER)").await.unwrap();
            assert!(path.exists());
        });
    }

    fn uri_flags() -> OpenFlags {
        OpenFlags::default_flags() | OpenFlags::SQLITE_OPEN_URI
    }

    #[test]
    fn uri_paths_are_decoded_after_delimiters_and_only_once() {
        for (uri, expected) in [
            ("file:data.db", "data.db"),
            ("file:/tmp/data.db", "/tmp/data.db"),
            ("file:///tmp/data.db", "/tmp/data.db"),
            ("file://localhost/tmp/data.db", "/tmp/data.db"),
            ("file:data%20+%23%3F%26%3D.db?mode=ro#ignored", "data +#?&=.db"),
            ("file:caf%C3%A9.db", "café.db"),
            ("file:escaped%253F.db", "escaped%3F.db"),
            ("file:literal%oops%.db", "literal%oops%.db"),
            ("file:a.db#?mode=ro", "a.db"),
        ] {
            let resolved = resolve_open(uri, uri_flags()).unwrap();
            assert_eq!(resolved.path, expected, "{uri}");
        }
        assert_eq!(
            resolve_open("file:a.db#?mode=ro", uri_flags()).unwrap().disposition,
            OpenDisposition::WriteCreate,
        );
        assert!(matches!(
            decode_uri_component("no allocation+needed").unwrap(),
            Cow::Borrowed(_)
        ));
    }

    #[test]
    fn ordinary_filenames_and_disabled_uri_flag_are_literal() {
        for path in ["ordinary?mode=ro#x", "FILE:a.db?mode=ro", "https:db?mode=ro"] {
            let resolved = resolve_open(path, uri_flags()).unwrap();
            assert_eq!(resolved.path, path);
            assert_eq!(resolved.disposition, OpenDisposition::WriteCreate);
        }
        let path = "file:literal%20.db?mode=ro";
        let resolved = resolve_open(path, OpenFlags::default_flags()).unwrap();
        assert_eq!(resolved.path, path);
        assert_eq!(resolved.disposition, OpenDisposition::WriteCreate);
    }

    #[test]
    fn uri_modes_narrow_the_callers_flags() {
        for (flags, allowed) in [
            (OpenFlags::SQLITE_OPEN_READ_ONLY, [true, false, false]),
            (OpenFlags::SQLITE_OPEN_READ_WRITE, [true, true, false]),
            (OpenFlags::default_flags(), [true, true, true]),
        ] {
            for ((mode, expected), allowed) in [
                ("ro", OpenDisposition::ReadOnly),
                ("rw", OpenDisposition::WriteExisting),
                ("rwc", OpenDisposition::WriteCreate),
            ].into_iter().zip(allowed) {
                let uri = format!("file:a.db?mode={mode}");
                let result = resolve_open(&uri, flags | OpenFlags::SQLITE_OPEN_URI);
                if allowed {
                    assert_eq!(result.unwrap().disposition, expected);
                } else {
                    assert!(result.unwrap_err().to_string().contains("access mode not allowed"));
                }
            }
        }
    }

    #[test]
    fn repeated_modes_cannot_launder_a_read_only_or_no_create_request() {
        for query in [
            "mode=ro&mode=rw",
            "mode=rw&mode=rwc",
            "mode=ro&mode=memory&mode=rw",
            "mode=rw&mode=memory&mode=rwc",
        ] {
            assert!(resolve_open(&format!("file:a.db?{query}"), uri_flags()).is_err());
        }
        let resolved = resolve_open("file:a.db?mode=rwc&mode=rw&mode=ro", uri_flags()).unwrap();
        assert_eq!(resolved.disposition, OpenDisposition::ReadOnly);
        assert!(resolve_open(
            "file:a.db?mode=memory&mode=rwc",
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_URI,
        ).is_err());
        assert!(resolve_open(
            "file:a.db?mode=memory",
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
        ).is_err(), "read-only requests must not produce writable memory connections");
    }

    #[test]
    fn uri_query_decoding_preserves_literal_plus_and_case() {
        let resolved = resolve_open("file:a.db?mo%64e=%72o&cache=pr%69vate", uri_flags()).unwrap();
        assert_eq!(resolved.disposition, OpenDisposition::ReadOnly);
        for query in ["mode=+ro", "mode=RO", "mode", "mode=readonly", "cache=PRIVATE", "cache="] {
            assert!(resolve_open(&format!("file:a.db?{query}"), uri_flags()).is_err());
        }
        let resolved = resolve_open("file:a.db?MODE=ro&custom=a%26mode%3Dro", uri_flags()).unwrap();
        assert_eq!(resolved.disposition, OpenDisposition::WriteCreate);
        assert_eq!(resolved.path, "a.db");
    }

    #[test]
    fn uri_rejects_remote_authorities_and_ambiguous_decoded_names() {
        for uri in [
            "file://remote/tmp/a.db", "file://LOCALHOST/tmp/a.db",
            "file://local%68ost/tmp/a.db", "file://localhost?mode=memory",
            "file:a%00.db", "file:a.db?mode=ro%00rwc", "file:a.db?mo%00de=ro",
            "file:%ff.db", "file:a.db?mode=%ff", "file:a\0.db",
        ] {
            assert!(resolve_open(uri, uri_flags()).is_err(), "{uri:?}");
        }
        assert!(resolve_open("plain\0name", OpenFlags::default_flags()).is_err());
    }

    #[test]
    fn uri_unsupported_vfs_requests_fail_instead_of_silently_changing_semantics() {
        for query in [
            "vfs=unix", "vfs=", "immutable=1", "immutable=TRUE", "nolock=1",
            "nolock=yes", "modeof=owner.db", "psow=0", "cache=shared",
            "mode=memory&cache=shared",
        ] {
            assert!(matches!(
                resolve_open(&format!("file:a.db?{query}"), uri_flags()),
                Err(FrankenError::NotImplemented(_))
            ), "{query}");
        }
        for query in ["immutable=0&nolock=0", "immutable=off&nolock=false", "immutable=&nolock=NO"] {
            assert!(resolve_open(&format!("file:a.db?{query}"), uri_flags()).is_ok());
        }
    }

    #[test]
    fn private_cache_overrides_a_prior_shared_cache_request() {
        let flags = uri_flags() | OpenFlags::SQLITE_OPEN_SHARED_CACHE;
        assert!(resolve_open("file:a.db?mode=memory", flags).is_err());
        let resolved = resolve_open("file:a.db?mode=memory&cache=private", flags).unwrap();
        assert_eq!(resolved.path, ":memory:");
        assert!(resolve_open("file:a.db?cache=private&cache=shared", uri_flags()).is_err());
        assert!(resolve_open("file:a.db?cache=shared&cache=private", uri_flags()).is_ok());
    }

    #[test]
    fn empty_file_uris_do_not_silently_become_persistent_databases() {
        for uri in ["file:", "file://localhost", "file:?mode=rwc"] {
            assert!(matches!(resolve_open(uri, uri_flags()), Err(FrankenError::NotImplemented(_))));
        }
        assert_eq!(resolve_open("file:?mode=memory", uri_flags()).unwrap().path, ":memory:");
        assert_eq!(resolve_open("file::memory:", uri_flags()).unwrap().path, ":memory:");
    }

    #[cfg(windows)]
    #[test]
    fn windows_uri_drive_prefix_is_not_a_top_level_directory() {
        assert_eq!(
            resolve_open("file:///C:/dir/data.db?mode=rw", uri_flags()).unwrap().path,
            "C:/dir/data.db",
        );
        assert_eq!(resolve_open("file:/not-a-drive/a.db", uri_flags()).unwrap().path, "/not-a-drive/a.db");
    }

    #[test]
    fn private_memory_uri_connections_are_independent_and_never_create_files() {
        asupersync::test_utils::run_test(|| async {
            let directory = tempfile::tempdir().unwrap().keep();
            let path = directory.join("not-on-disk.db");
            let uri = format!("{}?mode=memory&cache=private", file_uri(&path));
            let first = open_with_flags(&uri, uri_flags()).await.unwrap();
            first.execute("CREATE TABLE only_here(x)").await.unwrap();
            first.execute("INSERT INTO only_here VALUES(7)").await.unwrap();
            let second = open_with_flags(&uri, uri_flags()).await.unwrap();
            assert_eq!(first.path(), ":memory:");
            assert_eq!(second.path(), ":memory:");
            assert!(second.query("SELECT x FROM only_here").await.is_err());
            assert_eq!(first.query("SELECT x FROM only_here").await.unwrap()[0].get(0),
                Some(&crate::SqliteValue::Integer(7)));
            first.close().await.unwrap();
            second.close().await.unwrap();
            assert!(!path.exists());
        });
    }

    fn file_uri(path: &std::path::Path) -> String {
        use std::fmt::Write as _;

        let path = path.to_str().unwrap();
        #[cfg(windows)]
        let path = path.replace('\\', "/");
        let mut uri = String::from("file:");
        #[cfg(windows)]
        uri.push('/');
        for byte in path.bytes() {
            if byte.is_ascii_alphanumeric() || b"/-._~:+".contains(&byte) {
                uri.push(char::from(byte));
            } else {
                write!(uri, "%{byte:02X}").unwrap();
            }
        }
        uri
    }

    #[cfg(feature = "native")]
    #[test]
    fn uri_read_only_reads_the_decoded_file_and_refuses_writes() {
        asupersync::test_utils::run_test(|| async {
            let directory = tempfile::tempdir().unwrap().keep();
            let path = directory.join("uri+ café % #.db");
            let stock = rusqlite::Connection::open(&path).unwrap();
            stock.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES(42)").unwrap();
            drop(stock);
            let original = fsqlite_vfs::host_fs::read(&path).unwrap();
            let uri = format!("{}?mo%64e=%72o#mode=rwc", file_uri(&path));
            let conn = open_with_flags_with_env(&uri, uri_flags(), ConnectionEnv::default())
                .await.unwrap();
            assert_eq!(conn.query("SELECT x FROM t").await.unwrap()[0].get(0),
                Some(&crate::SqliteValue::Integer(42)));
            assert!(conn.execute("INSERT INTO t VALUES(99)").await.is_err());
            assert!(conn.execute("CREATE TABLE forbidden(x)").await.is_err());
            conn.close().await.unwrap();
            assert_eq!(fsqlite_vfs::host_fs::read(&path).unwrap(), original);
            let stock = rusqlite::Connection::open(&path).unwrap();
            assert_eq!(stock.query_row("SELECT count(*) FROM t", [], |row| row.get::<_, i64>(0)).unwrap(), 1);
        });
    }

    #[cfg(feature = "native")]
    #[test]
    fn uri_rw_requires_existing_file_and_rwc_creates_the_decoded_filename() {
        asupersync::test_utils::run_test(|| async {
            let directory = tempfile::tempdir().unwrap().keep();
            let path = directory.join("created with+URI.db");
            let uri = file_uri(&path);
            assert!(open_with_flags(&format!("{uri}?mode=rw"), uri_flags()).await.is_err());
            assert!(!path.exists());
            assert!(open_with_flags(&format!("{uri}?mode=ro"), uri_flags()).await.is_err());
            assert!(!path.exists());
            let conn = open_with_flags(&format!("{uri}?mode=rwc"), uri_flags()).await.unwrap();
            conn.execute("CREATE TABLE t(x)").await.unwrap();
            conn.close().await.unwrap();
            assert!(path.exists());
            let conn = open_with_flags(&format!("{uri}?mode=rw"), uri_flags()).await.unwrap();
            conn.execute("INSERT INTO t VALUES(7)").await.unwrap();
            conn.close().await.unwrap();
            let stock = rusqlite::Connection::open(&path).unwrap();
            assert_eq!(stock.query_row("SELECT x FROM t", [], |row| row.get::<_, i64>(0)).unwrap(), 7);
        });
    }

    #[cfg(feature = "native")]
    #[test]
    fn uri_refusals_never_open_or_create_the_target() {
        asupersync::test_utils::run_test(|| async {
            let directory = tempfile::tempdir().unwrap().keep();
            let path = directory.join("must-not-exist.db");
            let uri = file_uri(&path);
            for query in ["mode=bogus", "mode=ro&mode=rw", "vfs=unix", "nolock=1", "cache=shared", "mode=ro%00rwc"] {
                assert!(open_with_flags(&format!("{uri}?{query}"), uri_flags()).await.is_err());
                assert!(!path.exists(), "{query} created a database");
                assert!(fsqlite_vfs::host_fs::read_dir_paths(&directory).unwrap().is_empty(),
                    "{query} created a companion before validation");
            }
        });
    }

    #[cfg(feature = "native")]
    #[test]
    fn uri_disk_mode_admission_matches_stock_sqlite() {
        let directory = tempfile::tempdir().unwrap().keep();
        let path = directory.join("oracle.db");
        let stock = rusqlite::Connection::open(&path).unwrap();
        stock.execute_batch("CREATE TABLE t(x)").unwrap();
        drop(stock);
        let uri = file_uri(&path);
        for (ours, stock_flags) in [
            (OpenFlags::SQLITE_OPEN_READ_ONLY, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY),
            (OpenFlags::SQLITE_OPEN_READ_WRITE, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE),
            (OpenFlags::default_flags(), rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE),
        ] {
            for query in [
                "", "mode=ro", "mode=rw", "mode=rwc", "mode=readonly", "mode=",
                "mo%64e=%72o", "mode=ro&mode=rw", "mode=rw&mode=rwc",
                "mode=rwc&mode=rw&mode=ro", "custom=x&mode=ro", "mode=ro#mode=rw",
            ] {
                let uri = format!("{uri}?{query}");
                let resolved = resolve_open(&uri, ours | OpenFlags::SQLITE_OPEN_URI);
                let reference = rusqlite::Connection::open_with_flags(
                    &uri, stock_flags | rusqlite::OpenFlags::SQLITE_OPEN_URI,
                );
                assert_eq!(resolved.is_ok(), reference.is_ok(), "{uri}");
                if let (Ok(resolved), Ok(reference)) = (resolved, reference) {
                    let writable = reference.execute("INSERT INTO t VALUES(1)", []).is_ok();
                    assert_eq!(resolved.disposition != OpenDisposition::ReadOnly, writable, "{uri}");
                }
            }
        }
    }
}
