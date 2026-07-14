use std::path::{Path, PathBuf};
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
use std::{ffi::OsString, fs::File};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::DatabaseHeader;
use fsqlite_types::cx::Cx;
use fsqlite_types::value::SqliteValue;
use fsqlite_vdbe::codegen::TableSchema;
use fsqlite_vdbe::engine::MemDatabase;
#[cfg(all(not(target_arch = "wasm32"), feature = "native", unix))]
use fsqlite_vfs::UnixVfs as PlatformVfs;
#[cfg(all(not(target_arch = "wasm32"), feature = "native", target_os = "windows"))]
use fsqlite_vfs::WindowsVfs as PlatformVfs;
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
use fsqlite_vfs::{FileIdentity, Vfs, host_fs};

use crate::compat_persist::SqliteMasterEntry;
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
use crate::compat_persist::persist_to_reserved_sqlite_with_header_and_master_entries;

pub(crate) const ATTACHED_SCHEMA_UNSUPPORTED: &str = "VACUUM on attached schemas";
pub(crate) const NON_TEXT_FILENAME: &str = "non-text filename";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VacuumTargetKind {
    UserOutput,
    Discard,
    InternalRebuild,
}

/// An empty output file reserved atomically and bound to its open descriptor.
///
/// The retained handle prevents filesystem-identity reuse while the pager
/// reopens the path in `ReservedEmpty` mode. Every later path-based operation
/// checks the same identity first, so a replacement entry is never treated as
/// our output or cleanup target.
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
#[derive(Debug)]
pub(crate) struct VacuumTargetReservation {
    path: PathBuf,
    identity: FileIdentity,
    reservation: File,
    kind: VacuumTargetKind,
}

#[cfg(any(target_arch = "wasm32", not(feature = "native")))]
#[derive(Debug)]
pub(crate) struct VacuumTargetReservation {
    path: PathBuf,
    kind: VacuumTargetKind,
}

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
impl VacuumTargetReservation {
    fn reserve_exact(cx: &Cx, path: &Path, kind: VacuumTargetKind) -> Result<Self> {
        let vfs = PlatformVfs::new();
        let path = vfs.full_pathname(cx, path)?;
        let reservation = host_fs::reserve_new_file(&path)?;
        let Some(identity) = FileIdentity::from_file(&reservation)? else {
            // Native Unix and Windows VFSes promise stable descriptor
            // identities. Keep a broken platform implementation fail-closed
            // and preserve the reservation for diagnosis: without an
            // identity there is no safe basis for path cleanup.
            return Err(FrankenError::internal(format!(
                "VACUUM target reservation has no stable file identity: {}",
                path.display()
            )));
        };
        Ok(Self {
            path,
            identity,
            reservation,
            kind,
        })
    }

    fn reserve_random(
        cx: &Cx,
        source_path: &Path,
        label: &str,
        kind: VacuumTargetKind,
    ) -> Result<Self> {
        const MAX_RESERVATION_ATTEMPTS: usize = 128;

        let source = source_path;
        let directory = if source == Path::new(":memory:") {
            std::env::temp_dir()
        } else {
            source
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf()
        };
        let base_name = source
            .file_name()
            .filter(|name| !name.is_empty() && *name != ":memory:")
            .unwrap_or_else(|| std::ffi::OsStr::new("memory"));
        let vfs = PlatformVfs::new();

        for _ in 0..MAX_RESERVATION_ATTEMPTS {
            let mut nonce = [0_u8; 16];
            vfs.randomness(cx, &mut nonce);
            let nonce = nonce
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            let mut file_name = OsString::from(base_name);
            file_name.push(format!(".fsqlite-{label}-{nonce}.tmp"));
            let path = directory.join(file_name);
            match Self::reserve_exact(cx, &path, kind) {
                Ok(reservation) => return Ok(reservation),
                Err(FrankenError::Io(error))
                    if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }

        Err(FrankenError::CannotOpen { path: directory })
    }

    #[must_use]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub(crate) const fn identity(&self) -> FileIdentity {
        self.identity
    }

    #[must_use]
    pub(crate) const fn kind(&self) -> VacuumTargetKind {
        self.kind
    }

    fn current_path_has_reserved_identity(&self) -> Result<bool> {
        match host_fs::open_file(&self.path) {
            Ok(file) => Ok(FileIdentity::from_file(&file)? == Some(self.identity)),
            Err(FrankenError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    /// Make a successful user-facing `VACUUM INTO` output durable before the
    /// statement reports success, then prove the requested name still refers
    /// to the reserved file.
    pub(crate) fn finish_user_output(&self, cx: &Cx) -> Result<()> {
        if self.kind != VacuumTargetKind::UserOutput {
            return Err(FrankenError::internal(
                "only a user VACUUM INTO target can be finalized as output",
            ));
        }
        self.reservation.sync_all()?;
        PlatformVfs::new().sync_parent_directory(cx, &self.path)?;
        if !self.current_path_has_reserved_identity()? {
            return Err(FrankenError::CannotOpen {
                path: self.path.clone(),
            });
        }
        Ok(())
    }

    /// Remove an internal or failed output only while its pathname still maps
    /// to the exact object reserved by this value.
    pub(crate) fn cleanup_if_owned(&self, cx: &Cx) -> Result<bool> {
        if FileIdentity::from_file(&self.reservation)? != Some(self.identity) {
            return Err(FrankenError::internal(
                "VACUUM reservation descriptor changed identity",
            ));
        }
        if !fsqlite_vfs::cleanup_abandoned_private_database(&self.path, self.identity)? {
            return Ok(false);
        }
        PlatformVfs::new().sync_parent_directory(cx, &self.path)?;
        Ok(true)
    }
}

#[cfg(any(target_arch = "wasm32", not(feature = "native")))]
impl VacuumTargetReservation {
    #[must_use]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub(crate) const fn kind(&self) -> VacuumTargetKind {
        self.kind
    }

    pub(crate) fn finish_user_output(&self, _cx: &Cx) -> Result<()> {
        Err(FrankenError::not_implemented(
            "VACUUM requires native file support",
        ))
    }

    pub(crate) fn cleanup_if_owned(&self, _cx: &Cx) -> Result<bool> {
        Err(FrankenError::not_implemented(
            "VACUUM requires native file support",
        ))
    }
}

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
pub(crate) fn persist_compacted_database(
    cx: &Cx,
    target: &VacuumTargetReservation,
    schema: &[TableSchema],
    db: &MemDatabase,
    header: &DatabaseHeader,
    extra_master_entries: &[SqliteMasterEntry],
    original_ddl: &std::collections::HashMap<String, String>,
) -> Result<()> {
    persist_to_reserved_sqlite_with_header_and_master_entries(
        cx,
        target.path(),
        target.identity(),
        schema,
        db,
        header,
        extra_master_entries,
        original_ddl,
    )
}

#[cfg(any(target_arch = "wasm32", not(feature = "native")))]
pub(crate) fn persist_compacted_database(
    _cx: &Cx,
    _target: &VacuumTargetReservation,
    _schema: &[TableSchema],
    _db: &MemDatabase,
    _header: &DatabaseHeader,
    _extra_master_entries: &[SqliteMasterEntry],
    _original_ddl: &std::collections::HashMap<String, String>,
) -> Result<()> {
    Err(FrankenError::not_implemented(
        "VACUUM requires native file support",
    ))
}

pub(crate) fn resolve_vacuum_into_target(
    cx: &Cx,
    source_path: &str,
    target_value: &SqliteValue,
) -> Result<VacuumTargetReservation> {
    match target_value {
        #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
        SqliteValue::Text(path) if !path.is_empty() => {
            let requested_path = Path::new(&**path);
            VacuumTargetReservation::reserve_exact(cx, requested_path, VacuumTargetKind::UserOutput)
                .map_err(|error| match error {
                    FrankenError::Io(ref io_error)
                        if io_error.kind() == std::io::ErrorKind::AlreadyExists =>
                    {
                        FrankenError::CannotOpen {
                            path: requested_path.to_owned(),
                        }
                    }
                    other => other,
                })
        }
        #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
        SqliteValue::Text(_) => VacuumTargetReservation::reserve_random(
            cx,
            Path::new(source_path),
            "vacuum-into-discard",
            VacuumTargetKind::Discard,
        ),
        #[cfg(any(target_arch = "wasm32", not(feature = "native")))]
        SqliteValue::Text(_) => Err(FrankenError::not_implemented(
            "VACUUM requires native file support",
        )),
        _ => Err(FrankenError::FunctionError(NON_TEXT_FILENAME.to_owned())),
    }
}

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
pub(crate) fn reserve_temp_rebuild_target(
    cx: &Cx,
    source_path: &Path,
) -> Result<VacuumTargetReservation> {
    VacuumTargetReservation::reserve_random(
        cx,
        source_path,
        "vacuum-rebuild",
        VacuumTargetKind::InternalRebuild,
    )
}

#[cfg(any(target_arch = "wasm32", not(feature = "native")))]
pub(crate) fn reserve_temp_rebuild_target(
    _cx: &Cx,
    source_path: &Path,
) -> Result<VacuumTargetReservation> {
    let _ = source_path;
    Err(FrankenError::not_implemented(
        "VACUUM requires native file support",
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::connection::Connection;
    use fsqlite_types::DatabaseHeader;
    use fsqlite_types::cx::Cx;
    use fsqlite_types::value::SqliteValue;
    use fsqlite_vdbe::engine::MemDatabase;
    use fsqlite_vfs::host_fs;

    use super::{
        NON_TEXT_FILENAME, VacuumTargetKind, persist_compacted_database,
        reserve_temp_rebuild_target, resolve_vacuum_into_target,
    };

    #[test]
    fn test_vacuum_rebuilds_file_backed_database_and_preserves_header_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("vacuum-in-place.db");
        let db = db_path.to_string_lossy().into_owned();

        let conn = Connection::open_with_page_size(&db, 1024).unwrap();
        conn.execute("PRAGMA user_version = 321;").unwrap();
        conn.execute("PRAGMA application_id = 654321;").unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT);")
            .unwrap();

        let mut insert_sql = String::from("BEGIN;");
        for rowid in 1..=120_u32 {
            insert_sql.push_str(&format!(
                "INSERT INTO t(id, payload) VALUES ({rowid}, '{}');",
                "x".repeat(700)
            ));
        }
        insert_sql.push_str("COMMIT;");
        conn.execute_batch(&insert_sql).unwrap();
        conn.execute("DELETE FROM t WHERE id <= 100;").unwrap();
        drop(conn);

        let oracle_before = rusqlite::Connection::open(&db_path).unwrap();
        let freelist_before: i64 = oracle_before
            .query_row("PRAGMA freelist_count;", [], |row| row.get(0))
            .unwrap();
        assert!(
            freelist_before > 0,
            "expected deletions to create free pages before VACUUM"
        );
        drop(oracle_before);

        let conn = Connection::open(&db).unwrap();
        conn.execute("VACUUM;").unwrap();
        let rows = conn
            .query("SELECT COUNT(*), MIN(id), MAX(id) FROM t;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].values()[0],
            fsqlite_types::value::SqliteValue::Integer(20)
        );
        assert_eq!(
            rows[0].values()[1],
            fsqlite_types::value::SqliteValue::Integer(101)
        );
        assert_eq!(
            rows[0].values()[2],
            fsqlite_types::value::SqliteValue::Integer(120)
        );
        drop(conn);

        let oracle_after = rusqlite::Connection::open(&db_path).unwrap();
        let page_size: i64 = oracle_after
            .query_row("PRAGMA page_size;", [], |row| row.get(0))
            .unwrap();
        let user_version: i64 = oracle_after
            .query_row("PRAGMA user_version;", [], |row| row.get(0))
            .unwrap();
        let application_id: i64 = oracle_after
            .query_row("PRAGMA application_id;", [], |row| row.get(0))
            .unwrap();
        let freelist_after: i64 = oracle_after
            .query_row("PRAGMA freelist_count;", [], |row| row.get(0))
            .unwrap();
        let row_count: i64 = oracle_after
            .query_row("SELECT COUNT(*) FROM t;", [], |row| row.get(0))
            .unwrap();

        assert_eq!(page_size, 1024);
        assert_eq!(user_version, 321);
        assert_eq!(application_id, 654321);
        assert_eq!(freelist_after, 0);
        assert_eq!(row_count, 20);
    }

    #[test]
    fn test_vacuum_into_writes_compacted_copy_with_preserved_page_size_and_pragmas() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("vacuum-into-source.db");
        let target_path = dir.path().join("vacuum-into-target.db");
        let source = source_path.to_string_lossy().into_owned();
        let target = target_path.to_string_lossy().into_owned();

        let conn = Connection::open_with_page_size(&source, 8192).unwrap();
        conn.execute("PRAGMA user_version = 777;").unwrap();
        conn.execute("PRAGMA application_id = 888;").unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma');")
            .unwrap();
        conn.execute("DELETE FROM t WHERE id = 2;").unwrap();
        conn.execute_with_params(
            "VACUUM INTO ?1;",
            &[fsqlite_types::value::SqliteValue::Text(
                target.clone().into(),
            )],
        )
        .unwrap();
        drop(conn);

        let copied = rusqlite::Connection::open(&target_path).unwrap();
        let page_size: i64 = copied
            .query_row("PRAGMA page_size;", [], |row| row.get(0))
            .unwrap();
        let user_version: i64 = copied
            .query_row("PRAGMA user_version;", [], |row| row.get(0))
            .unwrap();
        let application_id: i64 = copied
            .query_row("PRAGMA application_id;", [], |row| row.get(0))
            .unwrap();
        let freelist_count: i64 = copied
            .query_row("PRAGMA freelist_count;", [], |row| row.get(0))
            .unwrap();
        let values: Vec<(i64, String)> = {
            let mut stmt = copied
                .prepare("SELECT id, payload FROM t ORDER BY id;")
                .unwrap();
            stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        };

        assert_eq!(page_size, 8192);
        assert_eq!(user_version, 777);
        assert_eq!(application_id, 888);
        assert_eq!(freelist_count, 0);
        assert_eq!(
            values,
            vec![(1, "alpha".to_owned()), (3, "gamma".to_owned())]
        );
    }

    #[test]
    fn test_resolve_vacuum_into_target_rejects_non_text_values() {
        for target_value in [
            SqliteValue::Null,
            SqliteValue::Integer(7),
            SqliteValue::Float(1.25),
            SqliteValue::Blob(Arc::<[u8]>::from(vec![0xAA, 0xBB])),
        ] {
            let err =
                resolve_vacuum_into_target(&Cx::new(), "source.db", &target_value).unwrap_err();
            assert_eq!(err.to_string(), NON_TEXT_FILENAME);
        }
    }

    #[test]
    fn test_resolve_vacuum_into_target_empty_text_uses_discard_sink() {
        let cx = Cx::new();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.db");
        let target = resolve_vacuum_into_target(
            &cx,
            source.to_str().unwrap(),
            &SqliteValue::Text("".into()),
        )
        .unwrap();
        assert!(
            !target.path().as_os_str().is_empty(),
            "empty VACUUM INTO targets should resolve to an internal discard sink"
        );
        assert_eq!(target.kind(), VacuumTargetKind::Discard);
        assert!(target.path().exists());
    }

    #[test]
    fn test_vacuum_into_empty_text_discards_without_owned_artifacts() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("discard-source.db");
        let source = source_path.to_string_lossy().into_owned();
        let conn = Connection::open(&source).unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'kept');").unwrap();
        conn.execute_with_params("VACUUM INTO ?1;", &[SqliteValue::Text("".into())])
            .unwrap();
        let rows = conn.query("SELECT value FROM t WHERE id=1;").unwrap();
        assert_eq!(rows[0].values()[0], SqliteValue::Text("kept".into()));

        let artifacts = host_fs::read_dir_paths(dir.path())
            .unwrap()
            .into_iter()
            .filter(|path| {
                path.file_name().is_some_and(|name| {
                    name.to_string_lossy()
                        .contains(".fsqlite-vacuum-into-discard-")
                })
            })
            .collect::<Vec<_>>();
        assert!(artifacts.is_empty(), "{artifacts:?}");
    }

    #[test]
    fn test_internal_reservations_are_random_and_cleanup_is_identity_bound() {
        let cx = Cx::new();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.db");
        let first = reserve_temp_rebuild_target(&cx, &source).unwrap();
        let second = reserve_temp_rebuild_target(&cx, &source).unwrap();
        assert_ne!(first.path(), second.path());
        assert_eq!(first.kind(), VacuumTargetKind::InternalRebuild);
        assert_eq!(second.kind(), VacuumTargetKind::InternalRebuild);
        assert!(first.path().exists());
        assert!(second.path().exists());
    }

    #[test]
    fn test_cleanup_refuses_replaced_reservation_and_preserves_replacement() {
        let cx = Cx::new();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.db");
        let target = reserve_temp_rebuild_target(&cx, &source).unwrap();
        let original_path = target.path().to_owned();
        let moved_path = dir.path().join("moved-owned-reservation.db");
        host_fs::rename(&original_path, &moved_path).unwrap();
        host_fs::write(&original_path, b"replacement-sentinel").unwrap();

        assert!(!target.cleanup_if_owned(&cx).unwrap());
        assert_eq!(
            host_fs::read(&original_path).unwrap(),
            b"replacement-sentinel"
        );
        assert!(moved_path.exists());
    }

    #[test]
    fn test_reserved_persistence_rejects_path_swap_before_mutation() {
        let cx = Cx::new();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.db");
        let target_path = dir.path().join("output.db");
        let target = resolve_vacuum_into_target(
            &cx,
            source.to_str().unwrap(),
            &SqliteValue::Text(target_path.to_string_lossy().into_owned().into()),
        )
        .unwrap();
        let moved_path = dir.path().join("moved-output-reservation.db");
        host_fs::rename(&target_path, &moved_path).unwrap();
        host_fs::write(&target_path, b"replacement-sentinel").unwrap();

        let error = persist_compacted_database(
            &cx,
            &target,
            &[],
            &MemDatabase::new(),
            &DatabaseHeader::default(),
            &[],
            &HashMap::new(),
        )
        .expect_err("a replaced reserved output path must fail before mutation");
        assert!(
            matches!(error, fsqlite_error::FrankenError::CannotOpen { .. }),
            "unexpected reservation mismatch error: {error}"
        );
        assert_eq!(
            host_fs::read(&target_path).unwrap(),
            b"replacement-sentinel"
        );
        assert!(moved_path.exists());
    }

    #[test]
    fn test_vacuum_into_existing_file_is_no_clobber() {
        let cx = Cx::new();
        let dir = tempfile::tempdir().unwrap();
        let target_path = dir.path().join("existing.db");
        host_fs::write(&target_path, b"existing-sentinel").unwrap();
        let error = resolve_vacuum_into_target(
            &cx,
            "source.db",
            &SqliteValue::Text(target_path.to_string_lossy().into_owned().into()),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            fsqlite_error::FrankenError::CannotOpen { ref path } if path == &target_path
        ));
        assert_eq!(host_fs::read(&target_path).unwrap(), b"existing-sentinel");
    }

    #[cfg(unix)]
    #[test]
    fn test_vacuum_into_dangling_symlink_is_no_clobber() {
        use std::os::unix::fs::symlink;

        let cx = Cx::new();
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing-target.db");
        let target_path = dir.path().join("dangling-output.db");
        symlink(&missing, &target_path).unwrap();

        let error = resolve_vacuum_into_target(
            &cx,
            "source.db",
            &SqliteValue::Text(target_path.to_string_lossy().into_owned().into()),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            fsqlite_error::FrankenError::CannotOpen { ref path } if path == &target_path
        ));
        assert_eq!(host_fs::read_link(&target_path).unwrap(), missing);
    }

    #[test]
    fn test_vacuum_into_null_parameter_reports_non_text_filename() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("vacuum-into-null-source.db");
        let source = source_path.to_string_lossy().into_owned();

        let conn = Connection::open(&source).unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT);")
            .unwrap();

        let err = conn
            .execute_with_params("VACUUM INTO ?1;", &[SqliteValue::Null])
            .unwrap_err();
        assert_eq!(err.to_string(), NON_TEXT_FILENAME);
    }

    #[test]
    fn test_vacuum_into_empty_text_succeeds_without_leaving_output_file() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("vacuum-into-empty-source.db");
        let source = source_path.to_string_lossy().into_owned();

        let conn = Connection::open(&source).unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'alpha'), (2, 'beta');")
            .unwrap();

        conn.execute("VACUUM INTO '';").unwrap();

        let discard_files: Vec<_> = fsqlite_vfs::host_fs::read_dir_paths(dir.path())
            .unwrap()
            .into_iter()
            .filter(|path| {
                path.file_name()
                    .map(|name| name.to_string_lossy())
                    .is_some_and(|name| name.contains(".fsqlite-vacuum-into-discard-"))
            })
            .collect();
        assert!(
            discard_files.is_empty(),
            "VACUUM INTO '' should clean up its temporary discard sink: {discard_files:?}"
        );

        let rows = conn
            .query("SELECT id, payload FROM t ORDER BY id;")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));
        assert_eq!(rows[1].values()[0], SqliteValue::Integer(2));
    }

    #[test]
    fn test_vacuum_in_place_removes_rebuild_temp_file() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("vacuum-temp-cleanup.db");
        let db = db_path.to_string_lossy().into_owned();

        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT);")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'alpha'), (2, 'beta');")
            .unwrap();

        conn.execute("VACUUM;").unwrap();

        let temp_prefix = format!(
            "{}.fsqlite-vacuum-",
            db_path.file_name().unwrap().to_string_lossy()
        );
        let temp_files: Vec<_> = fsqlite_vfs::host_fs::read_dir_paths(dir.path())
            .unwrap()
            .into_iter()
            .filter(|path| {
                path.file_name()
                    .map(|name| name.to_string_lossy())
                    .is_some_and(|name| name.starts_with(&temp_prefix) && name.ends_with(".tmp"))
            })
            .collect();
        assert!(
            temp_files.is_empty(),
            "VACUUM should not leave rebuild temp files behind: {temp_files:?}"
        );
    }

    #[test]
    fn test_vacuum_preserves_views_and_triggers_across_rebuild_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("vacuum-schema-objects.db");
        let db = db_path.to_string_lossy().into_owned();

        let conn = Connection::open(&db).unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT);")
            .unwrap();
        conn.execute("CREATE TABLE audit(id INTEGER PRIMARY KEY);")
            .unwrap();
        conn.execute("CREATE VIEW live_t AS SELECT id, payload FROM t WHERE id > 0;")
            .unwrap();
        conn.execute(
            "CREATE TRIGGER t_audit AFTER INSERT ON t BEGIN INSERT INTO audit(id) VALUES (NEW.id); END;",
        )
        .unwrap();
        conn.execute("INSERT INTO t(id, payload) VALUES (1, 'alpha');")
            .unwrap();

        conn.execute("VACUUM;").unwrap();

        let live_rows = conn
            .query("SELECT id, payload FROM live_t ORDER BY id;")
            .unwrap();
        assert_eq!(live_rows.len(), 1);
        assert_eq!(
            live_rows[0].values()[0],
            fsqlite_types::value::SqliteValue::Integer(1)
        );
        assert_eq!(
            live_rows[0].values()[1],
            fsqlite_types::value::SqliteValue::Text("alpha".into())
        );

        conn.execute("INSERT INTO t(id, payload) VALUES (2, 'beta');")
            .unwrap();
        let audit_rows = conn.query("SELECT id FROM audit ORDER BY id;").unwrap();
        assert_eq!(audit_rows.len(), 2);
        assert_eq!(
            audit_rows[0].values()[0],
            fsqlite_types::value::SqliteValue::Integer(1)
        );
        assert_eq!(
            audit_rows[1].values()[0],
            fsqlite_types::value::SqliteValue::Integer(2)
        );
        drop(conn);

        let sqlite = rusqlite::Connection::open(&db_path).unwrap();
        let schema_rows: Vec<(String, String)> = {
            let mut stmt = sqlite
                .prepare(
                    "SELECT type, name
                     FROM sqlite_master
                     WHERE name IN ('live_t', 't_audit')
                     ORDER BY type, name;",
                )
                .unwrap();
            stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        };
        assert_eq!(
            schema_rows,
            vec![
                ("trigger".to_owned(), "t_audit".to_owned()),
                ("view".to_owned(), "live_t".to_owned()),
            ]
        );

        sqlite
            .execute("INSERT INTO t(id, payload) VALUES (3, 'gamma');", [])
            .unwrap();
        let audit_ids: Vec<i64> = {
            let mut stmt = sqlite.prepare("SELECT id FROM audit ORDER BY id;").unwrap();
            stmt.query_map([], |row| row.get(0))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        };
        assert_eq!(audit_ids, vec![1, 2, 3]);
    }
}
