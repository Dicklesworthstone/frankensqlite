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

#[cfg_attr(
    any(target_arch = "wasm32", not(feature = "native")),
    expect(
        dead_code,
        reason = "non-native VACUUM paths report NotImplemented but connection dispatch still matches every target kind"
    )
)]
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
pub(crate) async fn persist_compacted_database(
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
    .await
}

#[cfg(any(target_arch = "wasm32", not(feature = "native")))]
#[allow(clippy::unused_async)]
pub(crate) async fn persist_compacted_database(
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
    #[cfg(any(target_arch = "wasm32", not(feature = "native")))]
    let _ = (cx, source_path);

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

    fn header_u32_be(path: &std::path::Path, offset: usize) -> u32 {
        let bytes = std::fs::read(path).unwrap();
        u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ])
    }

    fn env_usize(key: &str, default: usize) -> usize {
        std::env::var(key)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    // GH#347 / bd-6sscx: VACUUM INTO of an already-compact source must stay
    // COMPACT: freelist_count == 0, freelist_trunk == 0, page_count <= source.
    // Env-parameterized so the shape can be swept WITHOUT recompiling (the
    // shared target is a 12-minute CARGO_INCREMENTAL=0 rebuild): re-run the
    // built test binary directly with FSQ347_* set.
    #[test]
    fn test_vacuum_into_many_schema_objects_stays_compact_gh347() {
        asupersync::test_utils::run_test(|| async {
            // GH#347 default shape: few tables, MANY rows, at least one
            // non-monotonic index (the TEXT column `a` and the UNIQUE TEXT
            // column `u` sort differently from rowid). Enough rows per index that
            // its b-tree spans multiple pages is what forces the unsorted per-row
            // bulk-load to insert at random b-tree positions, leaving grossly
            // sparse pages (~14x the compact source at this shape). A few-rows /
            // many-tables shape keeps every index b-tree single-page and does NOT
            // reproduce, so the defaults must stay row-heavy. 2000 rows/table is
            // the fastest shape (~12s) that still reproduces with a wide margin;
            // FSQ347_ROWS=40000 additionally surfaces the freed-scratch freelist
            // *trunk* symptom (trunk=17073 at HEAD) at the cost of a ~6-min run.
            let tables = env_usize("FSQ347_TABLES", 8);
            let indexes = env_usize("FSQ347_INDEXES", 2);
            let rows = env_usize("FSQ347_ROWS", 2_000);
            let payload = env_usize("FSQ347_PAYLOAD", 0);
            let add_unique = env_usize("FSQ347_UNIQUE", 1) != 0;
            let without_rowid = env_usize("FSQ347_WITHOUT_ROWID", 0) != 0;
            let stock_compact = env_usize("FSQ347_STOCK_COMPACT", 1) != 0;
            // Strict compaction is the regression guard by default; env override
            // stays so the built binary can still be swept without recompiling.
            let strict = env_usize("FSQ347_STRICT", 1) != 0;

            let dir = tempfile::tempdir().unwrap();
            let source_path = dir.path().join("gh347-source.db");
            let image_path = dir.path().join("gh347-image.db"); // triggering image (compact)
            let fsq_copy_path = dir.path().join("gh347-fsq-copy.db"); // fresh path fsqlite opens
            let target_path = dir.path().join("gh347-target.db");
            let oracle_target_path = dir.path().join("gh347-oracle-target.db");
            let source = source_path.to_string_lossy().into_owned();
            let fsq_copy = fsq_copy_path.to_string_lossy().into_owned();
            let target = target_path.to_string_lossy().into_owned();

            // 1. Build the synthetic source through an ordinary fsqlite connection.
            let conn = Connection::open(&source).await.unwrap();
            let mut batch = String::from("BEGIN;");
            for i in 0..tables {
                let wor = if without_rowid { " WITHOUT ROWID" } else { "" };
                let uniq = if add_unique { ", u TEXT UNIQUE" } else { "" };
                let pk = if without_rowid {
                    "id INTEGER, a TEXT, b INTEGER, c TEXT, PRIMARY KEY(id)"
                } else {
                    "id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c TEXT"
                };
                batch.push_str(&format!("CREATE TABLE t{i}({pk}{uniq}){wor};"));
                for k in 0..indexes {
                    match k {
                        0 => batch.push_str(&format!("CREATE INDEX ix{i}_a ON t{i}(a);")),
                        1 => batch.push_str(&format!("CREATE INDEX ix{i}_b ON t{i}(b);")),
                        _ => batch.push_str(&format!(
                            "CREATE INDEX ix{i}_{k} ON t{i}(c, b, a);"
                        )),
                    }
                }
                for r in 1..=rows {
                    let pad = if payload > 0 {
                        "x".repeat(payload)
                    } else {
                        String::new()
                    };
                    if add_unique {
                        batch.push_str(&format!(
                            "INSERT INTO t{i}(id, a, b, c, u) VALUES ({r}, 'a{i}_{r}{pad}', {r}, 'c{i}_{r}', 'u{i}_{r}');"
                        ));
                    } else {
                        batch.push_str(&format!(
                            "INSERT INTO t{i}(id, a, b, c) VALUES ({r}, 'a{i}_{r}{pad}', {r}, 'c{i}_{r}');"
                        ));
                    }
                }
            }
            batch.push_str("COMMIT;");
            conn.execute_batch(&batch).await.unwrap();
            drop(conn);

            // 2. Produce the triggering IMAGE. To mirror GH#347 (an already
            // stock-compact image, freelist 0), optionally run stock VACUUM.
            if stock_compact {
                let c = rusqlite::Connection::open(&source_path).unwrap();
                c.execute("VACUUM INTO ?1;", [image_path.to_string_lossy().as_ref()])
                    .unwrap();
            } else {
                let c = rusqlite::Connection::open(&source_path).unwrap();
                c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").ok();
                drop(c);
                std::fs::copy(&source_path, &image_path).unwrap();
            }

            // 3. Copy the image to a fresh path an ordinary connection opens.
            std::fs::copy(&image_path, &fsq_copy_path).unwrap();

            // Image (source) numbers, authoritative via stock sqlite3.
            let oracle_img = rusqlite::Connection::open(&image_path).unwrap();
            let img_page_count: i64 = oracle_img
                .query_row("PRAGMA page_count;", [], |row| row.get(0))
                .unwrap();
            let img_freelist: i64 = oracle_img
                .query_row("PRAGMA freelist_count;", [], |row| row.get(0))
                .unwrap();
            // Cross-check: stock sqlite3 VACUUM INTO of the SAME image is compact.
            oracle_img
                .execute(
                    "VACUUM INTO ?1;",
                    [oracle_target_path.to_string_lossy().as_ref()],
                )
                .unwrap();
            drop(oracle_img);
            let oracle_out_freelist: i64 = {
                let c = rusqlite::Connection::open(&oracle_target_path).unwrap();
                c.query_row("PRAGMA freelist_count;", [], |row| row.get(0))
                    .unwrap()
            };

            // 4. fsqlite VACUUM INTO on the copied image.
            let conn = Connection::open(&fsq_copy).await.unwrap();
            conn.execute_with_params(
                "VACUUM INTO ?1;",
                &[SqliteValue::Text(target.clone().into())],
            )
            .await
            .unwrap();
            drop(conn);

            // 5. Measure the fsqlite output header directly from disk.
            let out_page_count = header_u32_be(&target_path, 28);
            let out_freelist_trunk = header_u32_be(&target_path, 32);
            let out_freelist_count = header_u32_be(&target_path, 36);

            let oracle_out = rusqlite::Connection::open(&target_path).unwrap();
            let out_integrity: String = oracle_out
                .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
                .unwrap();
            drop(oracle_out);

            // Mirror the strict assertion's tolerance so the diagnostic label
            // agrees with pass/fail: a compact output within 15% of source is
            // NOT a reproduction, even though it is a few pages over stock.
            let reproduced = out_freelist_count != 0
                || out_freelist_trunk != 0
                || i64::from(out_page_count) * 100 > img_page_count * 115;

            eprintln!(
                "GH#347[tables={tables} idx={indexes} rows={rows} payload={payload} \
                 unique={add_unique} wor={without_rowid} stock_compact={stock_compact}] \
                 image: page_count={img_page_count} freelist={img_freelist} | \
                 stock_out_freelist={oracle_out_freelist} | \
                 fsqlite_out: page_count={out_page_count} trunk={out_freelist_trunk} \
                 freelist={out_freelist_count} integrity={out_integrity} => \
                 {}",
                if reproduced { "BUG REPRODUCED" } else { "compact-ok" }
            );

            assert_eq!(out_integrity, "ok", "output must stay sound");
            assert_eq!(
                oracle_out_freelist, 0,
                "stock sqlite3 VACUUM INTO must be compact"
            );
            if strict {
                // Canonical compaction signals: stock VACUUM leaves the freelist
                // empty. The pre-fix unsorted bulk-load freed balance-scratch
                // pages that serialized as a non-empty freelist trunk (trunk=17073
                // freelist=144 at the 40000-row shape); the fix must drive both to
                // zero.
                assert_eq!(
                    out_freelist_count, 0,
                    "GH#347: VACUUM INTO output must have an empty freelist (count={out_freelist_count} trunk={out_freelist_trunk})"
                );
                assert_eq!(
                    out_freelist_trunk, 0,
                    "GH#347: VACUUM INTO output must not invent a freelist trunk"
                );
                // Compaction: the pre-fix output is 2.7x-14x the compact source
                // (random-position index inserts leave grossly sparse pages). The
                // fix inserts index keys in sorted b-tree order, collapsing that to
                // within ~3% of source — the residual is fsqlite's slightly looser
                // index-page fill vs stock's balance-quick, a distinct packing
                // matter, not the GH#347 non-compaction defect. Bound at 1.15x so
                // the assertion isolates the defect (fails at 2.7x-14x) without
                // demanding byte-for-byte parity with stock's packing.
                assert!(
                    i64::from(out_page_count) * 100 <= img_page_count * 115,
                    "GH#347: VACUUM INTO output ({out_page_count}) must stay within 15% of the \
                     compact source ({img_page_count}); a larger image means the index bulk-load \
                     is not inserting keys in sorted b-tree order"
                );
            }
        });
    }

    // GH#340 / bd-m0e3b: VACUUM INTO must produce valid WITHOUT ROWID table
    // roots. A WITHOUT ROWID table's primary structure is an index b-tree, not a
    // table b-tree; the vacuumed image must record/serialize that root correctly
    // so stock sqlite3 reads it as `ok` and the rows survive.
    #[test]
    fn vacuum_into_preserves_without_rowid_table_roots() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let source_path = dir.path().join("wor-vacuum-source.db");
            let target_path = dir.path().join("wor-vacuum-target.db");
            let source = source_path.to_string_lossy().into_owned();
            let target = target_path.to_string_lossy().into_owned();

            let conn = Connection::open(&source).await.unwrap();
            conn.execute("CREATE TABLE t(k TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID;")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES ('a', '1'), ('b', '2'), ('c', '3');")
                .await
                .unwrap();
            conn.execute_with_params(
                "VACUUM INTO ?1;",
                &[SqliteValue::Text(target.clone().into())],
            )
            .await
            .unwrap();
            drop(conn);

            // Stock sqlite3 must accept the vacuumed WITHOUT ROWID image.
            let copied = rusqlite::Connection::open(&target_path).unwrap();
            let integrity_check: String = copied
                .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
                .unwrap();
            assert_eq!(
                integrity_check, "ok",
                "GH#340: VACUUM INTO produced a WITHOUT ROWID image stock sqlite3 rejects: {integrity_check}"
            );
            let rows: Vec<(String, String)> = {
                let mut stmt = copied.prepare("SELECT k, v FROM t ORDER BY k;").unwrap();
                let mapped = stmt
                    .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
                    .unwrap();
                mapped.map(Result::unwrap).collect()
            };
            assert_eq!(
                rows,
                vec![
                    ("a".to_string(), "1".to_string()),
                    ("b".to_string(), "2".to_string()),
                    ("c".to_string(), "3".to_string()),
                ]
            );

            // fsqlite must also read the vacuumed image back.
            let reopened = Connection::open_schema_only(&target).await.unwrap();
            let frows = reopened
                .query("SELECT k, v FROM t ORDER BY k;")
                .await
                .unwrap();
            assert_eq!(frows.len(), 3);
            assert_eq!(frows[0].values()[0], SqliteValue::Text("a".into()));
            reopened.close().await.unwrap();
        });
    }

    #[test]
    fn test_vacuum_rebuilds_file_backed_database_and_preserves_header_metadata() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("vacuum-in-place.db");
            let db = db_path.to_string_lossy().into_owned();

            let conn = Connection::open_with_page_size(&db, 1024).await.unwrap();
            conn.execute("PRAGMA user_version = 321;").await.unwrap();
            conn.execute("PRAGMA application_id = 654321;")
                .await
                .unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT);")
                .await
                .unwrap();

            let mut insert_sql = String::from("BEGIN;");
            for rowid in 1..=120_u32 {
                insert_sql.push_str(&format!(
                    "INSERT INTO t(id, payload) VALUES ({rowid}, '{}');",
                    "x".repeat(700)
                ));
            }
            insert_sql.push_str("COMMIT;");
            conn.execute_batch(&insert_sql).await.unwrap();
            conn.execute("DELETE FROM t WHERE id <= 100;")
                .await
                .unwrap();
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

            let conn = Connection::open(&db).await.unwrap();
            conn.execute("VACUUM;").await.unwrap();
            let rows = conn
                .query("SELECT COUNT(*), MIN(id), MAX(id) FROM t;")
                .await
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
        });
    }

    #[test]
    fn test_vacuum_into_writes_compacted_copy_with_preserved_page_size_and_pragmas() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let source_path = dir.path().join("vacuum-into-source.db");
            let target_path = dir.path().join("vacuum-into-target.db");
            let source = source_path.to_string_lossy().into_owned();
            let target = target_path.to_string_lossy().into_owned();

            let conn = Connection::open_with_page_size(&source, 8192)
                .await
                .unwrap();
            conn.execute("PRAGMA user_version = 777;").await.unwrap();
            conn.execute("PRAGMA application_id = 888;").await.unwrap();
            conn.execute(
                "CREATE TABLE t(\
                id INTEGER PRIMARY KEY,\
                payload TEXT NOT NULL UNIQUE CHECK(length(payload) > 0)\
            );",
            )
            .await
            .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma');")
                .await
                .unwrap();
            conn.execute("DELETE FROM t WHERE id = 2;").await.unwrap();
            conn.execute_with_params(
                "VACUUM INTO ?1;",
                &[fsqlite_types::value::SqliteValue::Text(
                    target.clone().into(),
                )],
            )
            .await
            .unwrap();
            drop(conn);

            let copied_fsqlite = Connection::open_schema_only(&target).await.unwrap();
            let copied_rows = copied_fsqlite
                .query("SELECT id, payload FROM t ORDER BY id;")
                .await
                .unwrap();
            assert_eq!(copied_rows.len(), 2);
            assert_eq!(copied_rows[0].values()[0], SqliteValue::Integer(1));
            assert_eq!(
                copied_rows[0].values()[1],
                SqliteValue::Text("alpha".into())
            );
            assert_eq!(copied_rows[1].values()[0], SqliteValue::Integer(3));
            assert_eq!(
                copied_rows[1].values()[1],
                SqliteValue::Text("gamma".into())
            );
            let copied_schema = copied_fsqlite
                .query("SELECT sql FROM sqlite_master WHERE type='table' AND name='t';")
                .await
                .unwrap();
            let copied_ddl = copied_schema[0].values()[0].to_text().to_ascii_uppercase();
            assert!(copied_ddl.contains("UNIQUE"), "{copied_ddl}");
            assert!(copied_ddl.contains("CHECK"), "{copied_ddl}");
            copied_fsqlite.close().await.unwrap();

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
            let quick_check: String = copied
                .query_row("PRAGMA quick_check;", [], |row| row.get(0))
                .unwrap();
            let integrity_check: String = copied
                .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
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
            assert_eq!(quick_check, "ok");
            assert_eq!(integrity_check, "ok");
            assert_eq!(
                values,
                vec![(1, "alpha".to_owned()), (3, "gamma".to_owned())]
            );
            assert!(
                copied
                    .execute("INSERT INTO t VALUES (4, 'alpha');", [])
                    .is_err(),
                "the VACUUM INTO output must preserve the UNIQUE constraint"
            );
        });
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
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let source_path = dir.path().join("discard-source.db");
            let source = source_path.to_string_lossy().into_owned();
            let conn = Connection::open(&source).await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'kept');")
                .await
                .unwrap();
            conn.execute_with_params("VACUUM INTO ?1;", &[SqliteValue::Text("".into())])
                .await
                .unwrap();
            let rows = conn.query("SELECT value FROM t WHERE id=1;").await.unwrap();
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
        });
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

    /// GH #304 (cleanup arm): when the rebuild refuses an index whose declared
    /// collation the builder cannot resolve, the partially written candidate
    /// must actually be removed, and an unrelated caller-owned file in the same
    /// directory must be left byte-for-byte alone.
    ///
    /// `test_cleanup_refuses_replaced_reservation_and_preserves_replacement`
    /// already covers the negative half — cleanup declines a path whose identity
    /// no longer matches. This covers the positive half, which nothing asserted:
    /// that a genuine mid-rebuild failure leaves a candidate the reservation
    /// then reclaims. The helper-level `db_path.exists()` assertion in
    /// `compat_persist` documents only that the persist path does not delete its
    /// own output; it is not a cleanup proof, and this is.
    ///
    /// Scope, stated narrowly: this proves the **explicit** reservation cleanup
    /// arm behaves correctly when it is invoked — `cleanup_if_owned` reclaims a
    /// candidate it still owns, spares an unrelated path, and leaves the source
    /// image untouched. It does **not** prove that every enclosing VACUUM caller
    /// reaches that call on every failure path; wiring coverage over the callers
    /// themselves is separate, still-open GH #304 work.
    #[test]
    fn test_failed_rebuild_candidate_is_reclaimed_and_bystanders_are_untouched() {
        asupersync::test_utils::run_test(|| async {
            use fsqlite_ast::SortDirection;
            use fsqlite_vdbe::codegen::{ColumnInfo, IndexSchema, TableSchema};

            let cx = Cx::new();
            let dir = tempfile::tempdir().unwrap();

            // The source must actually exist and carry known bytes, otherwise
            // "the source image is unchanged" — a GH #304 acceptance criterion —
            // is unprovable: an absent file trivially satisfies any comparison.
            let source = dir.path().join("source.db");
            const SOURCE_SENTINEL: &[u8] = b"source-image-sentinel-do-not-touch";
            host_fs::write(&source, SOURCE_SENTINEL).unwrap();

            // A pre-existing, caller-owned file that the rebuild has no claim
            // over. It sits in the same directory the internal reservation is
            // drawn from, so an identity-blind cleanup would be free to take it.
            let bystander = dir.path().join("caller-owned.db");
            host_fs::write(&bystander, b"caller-owned-sentinel").unwrap();

            let target = reserve_temp_rebuild_target(&cx, &source).unwrap();
            let candidate = target.path().to_owned();
            assert!(
                candidate.exists(),
                "reservation must materialize its target"
            );

            let mut db = MemDatabase::new();
            db.create_table_at(2, 2);
            let table = db.get_table_mut(2).unwrap();
            for id in 1..=4_i64 {
                table.insert_row(
                    id,
                    vec![
                        SqliteValue::Integer(id),
                        SqliteValue::Text(format!("v{id}").into()),
                    ],
                );
            }

            let schema = vec![TableSchema {
                name: "t".to_owned(),
                root_page: 2,
                columns: vec![
                    ColumnInfo::basic("id", 'D', true),
                    ColumnInfo::basic("label", 'B', false),
                ],
                indexes: vec![IndexSchema {
                    name: "idx_t_label_custom".to_owned(),
                    root_page: 3,
                    columns: vec!["label".to_owned()],
                    key_expressions: Vec::new(),
                    key_sort_directions: vec![SortDirection::Asc],
                    where_clause: None,
                    is_unique: false,
                    // Unresolvable by the builder's registry: the rebuild must
                    // refuse rather than silently order this index by BINARY.
                    key_collations: vec![Some("MYCOLL".to_owned())],
                    conflict_action: None,
                }],
                strict: false,
                without_rowid: false,
                primary_key_constraints: vec![vec!["id".to_owned()]],
                foreign_keys: Vec::new(),
                check_constraints: Vec::new(),
            }];
            let mut original_ddl = HashMap::new();
            original_ddl.insert(
                "t".to_owned(),
                "CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT)".to_owned(),
            );
            original_ddl.insert(
                "idx_t_label_custom".to_owned(),
                "CREATE INDEX idx_t_label_custom ON t(label COLLATE MYCOLL)".to_owned(),
            );

            let error = persist_compacted_database(
                &cx,
                &target,
                &schema,
                &db,
                &DatabaseHeader::default(),
                &[],
                &original_ddl,
            )
            .await
            .expect_err("an unresolvable index collation must fail the rebuild");
            assert!(
                matches!(
                    error,
                    fsqlite_error::FrankenError::NotImplemented(ref detail)
                        if detail.contains("MYCOLL")
                ),
                "unexpected rebuild failure: {error}"
            );

            // The persist path does not remove its own output, so the candidate
            // is still present at this point; the reservation owns reclaiming it.
            assert!(
                candidate.exists(),
                "the failed rebuild must leave its candidate for the reservation"
            );
            assert!(
                target.cleanup_if_owned(&cx).unwrap(),
                "the reservation still owns the candidate and must reclaim it"
            );
            assert!(
                !candidate.exists(),
                "the partially written candidate must be gone after cleanup"
            );

            // GH #304 acceptance: a failed rebuild must leave the source image
            // byte-for-byte intact.
            assert!(source.exists(), "the source image must survive the failure");
            assert_eq!(
                host_fs::read(&source).unwrap(),
                SOURCE_SENTINEL,
                "a failed rebuild must not modify the source image"
            );

            // The bystander must survive untouched in both existence and bytes.
            assert!(
                bystander.exists(),
                "cleanup must not remove unrelated files"
            );
            assert_eq!(
                host_fs::read(&bystander).unwrap(),
                b"caller-owned-sentinel",
                "cleanup must not overwrite unrelated files"
            );
        });
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
        asupersync::test_utils::run_test(|| async {
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
            .await
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
        });
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
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let source_path = dir.path().join("vacuum-into-null-source.db");
            let source = source_path.to_string_lossy().into_owned();

            let conn = Connection::open(&source).await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT);")
                .await
                .unwrap();

            let err = conn
                .execute_with_params("VACUUM INTO ?1;", &[SqliteValue::Null])
                .await
                .unwrap_err();
            assert_eq!(err.to_string(), NON_TEXT_FILENAME);
        });
    }

    #[test]
    fn test_vacuum_into_empty_text_succeeds_without_leaving_output_file() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let source_path = dir.path().join("vacuum-into-empty-source.db");
            let source = source_path.to_string_lossy().into_owned();

            let conn = Connection::open(&source).await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'alpha'), (2, 'beta');")
                .await
                .unwrap();

            conn.execute("VACUUM INTO '';").await.unwrap();

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
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));
            assert_eq!(rows[1].values()[0], SqliteValue::Integer(2));
        });
    }

    #[test]
    fn test_vacuum_in_place_removes_rebuild_temp_file() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("vacuum-temp-cleanup.db");
            let db = db_path.to_string_lossy().into_owned();

            let conn = Connection::open(&db).await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'alpha'), (2, 'beta');")
                .await
                .unwrap();

            conn.execute("VACUUM;").await.unwrap();

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
                        .is_some_and(|name| {
                            name.starts_with(&temp_prefix) && name.ends_with(".tmp")
                        })
                })
                .collect();
            assert!(
                temp_files.is_empty(),
                "VACUUM should not leave rebuild temp files behind: {temp_files:?}"
            );
        });
    }

    #[test]
    fn test_vacuum_preserves_views_and_triggers_across_rebuild_and_reopen() {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("vacuum-schema-objects.db");
            let db = db_path.to_string_lossy().into_owned();

            let conn = Connection::open(&db).await.unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT);")
                .await
                .unwrap();
            conn.execute("CREATE TABLE audit(id INTEGER PRIMARY KEY);")
                .await
                .unwrap();
            conn.execute("CREATE VIEW live_t AS SELECT id, payload FROM t WHERE id > 0;")
                .await
                .unwrap();
            conn.execute(
                "CREATE TRIGGER t_audit AFTER INSERT ON t BEGIN INSERT INTO audit(id) VALUES (NEW.id); END;",
            )
            .await
            .unwrap();
            conn.execute("INSERT INTO t(id, payload) VALUES (1, 'alpha');")
                .await
                .unwrap();

            conn.execute("VACUUM;").await.unwrap();

            let live_rows = conn
                .query("SELECT id, payload FROM live_t ORDER BY id;")
                .await
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
                .await
                .unwrap();
            let audit_rows = conn
                .query("SELECT id FROM audit ORDER BY id;")
                .await
                .unwrap();
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
        });
    }
}
