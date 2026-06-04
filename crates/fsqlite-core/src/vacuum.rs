#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
use std::collections::HashSet;
use std::path::{Path, PathBuf};
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
use std::sync::{
    Mutex, OnceLock,
    atomic::{AtomicU64, Ordering},
};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::DatabaseHeader;
use fsqlite_types::cx::Cx;
use fsqlite_types::value::SqliteValue;
use fsqlite_vdbe::codegen::TableSchema;
use fsqlite_vdbe::engine::MemDatabase;
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
use fsqlite_vfs::host_fs;

use crate::compat_persist::SqliteMasterEntry;
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
use crate::compat_persist::persist_to_sqlite_with_header_and_master_entries;

pub(crate) const ATTACHED_SCHEMA_UNSUPPORTED: &str = "VACUUM on attached schemas";
pub(crate) const NON_TEXT_FILENAME: &str = "non-text filename";

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
static NEXT_TEMP_REBUILD_ID: AtomicU64 = AtomicU64::new(1);
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
static NEXT_TEMP_VACUUM_INTO_DISCARD_ID: AtomicU64 = AtomicU64::new(1);
#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
static TEMP_VACUUM_INTO_DISCARD_TARGETS: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
fn temp_vacuum_into_discard_targets() -> &'static Mutex<HashSet<PathBuf>> {
    TEMP_VACUUM_INTO_DISCARD_TARGETS.get_or_init(|| Mutex::new(HashSet::new()))
}

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
fn register_temp_vacuum_into_discard_target(path: &Path) {
    let mut targets = temp_vacuum_into_discard_targets()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    targets.insert(path.to_path_buf());
}

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
fn take_temp_vacuum_into_discard_target(path: &Path) -> bool {
    let mut targets = temp_vacuum_into_discard_targets()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    targets.remove(path)
}

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
fn resolve_empty_vacuum_into_target(source_path: &str) -> PathBuf {
    let seq = NEXT_TEMP_VACUUM_INTO_DISCARD_ID.fetch_add(1, Ordering::Relaxed);
    let source = Path::new(source_path);
    let file_name = format!(
        "{}.fsqlite-vacuum-into-discard-{seq}.tmp",
        source
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty() && *name != ":memory:")
            .unwrap_or("memory")
    );
    let path = if source == Path::new(":memory:") {
        std::env::temp_dir().join(file_name)
    } else {
        source.with_file_name(file_name)
    };
    register_temp_vacuum_into_discard_target(&path);
    path
}

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
pub(crate) fn persist_compacted_database(
    cx: &Cx,
    target_path: &Path,
    schema: &[TableSchema],
    db: &MemDatabase,
    header: &DatabaseHeader,
    extra_master_entries: &[SqliteMasterEntry],
    original_ddl: &std::collections::HashMap<String, String>,
) -> Result<()> {
    let result = persist_to_sqlite_with_header_and_master_entries(
        cx,
        target_path,
        schema,
        db,
        header,
        extra_master_entries,
        original_ddl,
    );
    if take_temp_vacuum_into_discard_target(target_path) {
        drop(host_fs::remove_file(target_path));
    }
    result
}

#[cfg(any(target_arch = "wasm32", not(feature = "native")))]
pub(crate) fn persist_compacted_database(
    _cx: &Cx,
    _target_path: &Path,
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

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
pub(crate) fn validate_vacuum_into_target(source_path: &str, target_path: &Path) -> Result<()> {
    if target_path.as_os_str().is_empty() {
        return Err(FrankenError::CannotOpen {
            path: target_path.to_path_buf(),
        });
    }
    if target_path == Path::new(source_path) || host_fs::metadata(target_path).is_ok() {
        return Err(FrankenError::CannotOpen {
            path: target_path.to_path_buf(),
        });
    }
    Ok(())
}

pub(crate) fn resolve_vacuum_into_target(
    source_path: &str,
    target_value: &SqliteValue,
) -> Result<PathBuf> {
    let target_path = match target_value {
        SqliteValue::Text(path) if !path.is_empty() => PathBuf::from(&**path),
        #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
        SqliteValue::Text(_) => resolve_empty_vacuum_into_target(source_path),
        #[cfg(any(target_arch = "wasm32", not(feature = "native")))]
        SqliteValue::Text(_) => {
            return Err(FrankenError::CannotOpen {
                path: PathBuf::new(),
            });
        }
        _ => return Err(FrankenError::FunctionError(NON_TEXT_FILENAME.to_owned())),
    };
    if let Err(err) = validate_vacuum_into_target(source_path, &target_path) {
        #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
        let _ = take_temp_vacuum_into_discard_target(&target_path);
        return Err(err);
    }
    Ok(target_path)
}

#[cfg(any(target_arch = "wasm32", not(feature = "native")))]
pub(crate) fn validate_vacuum_into_target(_source_path: &str, target_path: &Path) -> Result<()> {
    if target_path.as_os_str().is_empty() {
        return Err(FrankenError::CannotOpen {
            path: target_path.to_path_buf(),
        });
    }
    Err(FrankenError::not_implemented(
        "VACUUM requires native file support",
    ))
}

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
pub(crate) fn temp_rebuild_path(source_path: &Path) -> PathBuf {
    let seq = NEXT_TEMP_REBUILD_ID.fetch_add(1, Ordering::Relaxed);
    let mut name = source_path
        .file_name()
        .map_or_else(|| "main".into(), std::ffi::OsString::from);
    name.push(format!(".fsqlite-vacuum-{seq}.tmp"));
    source_path.with_file_name(name)
}

#[cfg(any(target_arch = "wasm32", not(feature = "native")))]
pub(crate) fn temp_rebuild_path(source_path: &Path) -> PathBuf {
    source_path.to_path_buf()
}

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
pub(crate) fn replace_database_file(target_path: &Path, rebuilt_path: &Path) -> Result<()> {
    match host_fs::copy_file(rebuilt_path, target_path) {
        Ok(_) => {
            host_fs::remove_file(rebuilt_path)?;
            Ok(())
        }
        Err(err) => {
            drop(host_fs::remove_file(rebuilt_path));
            Err(err)
        }
    }
}

#[cfg(any(target_arch = "wasm32", not(feature = "native")))]
pub(crate) fn replace_database_file(_target_path: &Path, _rebuilt_path: &Path) -> Result<()> {
    Err(FrankenError::not_implemented(
        "VACUUM requires native file support",
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::connection::Connection;
    use fsqlite_types::value::SqliteValue;

    use super::{
        NON_TEXT_FILENAME, resolve_vacuum_into_target, take_temp_vacuum_into_discard_target,
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
            let err = resolve_vacuum_into_target("source.db", &target_value).unwrap_err();
            assert_eq!(err.to_string(), NON_TEXT_FILENAME);
        }
    }

    #[test]
    fn test_resolve_vacuum_into_target_empty_text_uses_discard_sink() {
        let target_path =
            resolve_vacuum_into_target("source.db", &SqliteValue::Text("".into())).unwrap();
        assert!(
            !target_path.as_os_str().is_empty(),
            "empty VACUUM INTO targets should resolve to an internal discard sink"
        );
        assert!(
            take_temp_vacuum_into_discard_target(&target_path),
            "discard sink should be tracked for cleanup"
        );
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
    fn test_replace_database_file_cleans_up_temp_file_on_write_error() {
        let dir = tempfile::tempdir().unwrap();
        let target_dir = dir.path().join("vacuum-target-dir");
        let rebuilt_path = dir.path().join("vacuum-target.db.fsqlite-vacuum-1.tmp");

        fsqlite_vfs::host_fs::create_dir_all(&target_dir).unwrap();
        fsqlite_vfs::host_fs::write(&rebuilt_path, b"rebuilt database bytes").unwrap();

        let err = super::replace_database_file(&target_dir, &rebuilt_path)
            .expect_err("directory targets must fail replacement");
        assert!(
            !rebuilt_path.exists(),
            "failed replacements must still remove the rebuild temp file"
        );
        assert!(
            err.to_string().contains("Is a directory") || err.to_string().contains("directory"),
            "unexpected replacement failure: {err}"
        );
    }

    #[test]
    fn test_replace_database_file_replaces_target_contents_without_buffering_entire_db() {
        let dir = tempfile::tempdir().unwrap();
        let target_path = dir.path().join("vacuum-target.db");
        let rebuilt_path = dir.path().join("vacuum-target.db.fsqlite-vacuum-2.tmp");

        fsqlite_vfs::host_fs::write(&target_path, b"old target bytes").unwrap();
        fsqlite_vfs::host_fs::write(&rebuilt_path, b"rebuilt database bytes").unwrap();

        super::replace_database_file(&target_path, &rebuilt_path).unwrap();

        assert_eq!(
            fsqlite_vfs::host_fs::read(&target_path).unwrap(),
            b"rebuilt database bytes"
        );
        assert!(
            !rebuilt_path.exists(),
            "successful replacements must remove the rebuild temp file"
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
