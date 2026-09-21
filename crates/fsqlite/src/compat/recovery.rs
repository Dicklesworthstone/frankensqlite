//! Explicit WAL-FEC repair followed by an identity-bound SQL connection open.
//!
//! This is a FrankenSQLite administrative extension, not a rusqlite API. It
//! does not enable automatic mutation on ordinary `Connection::open`. The
//! source must be an existing WAL-mode database and the backup path must be
//! new. Preserve the main/FEC/certificate set too: a WAL backup alone is not a
//! complete database backup. Run `PRAGMA integrity_check` before using repaired
//! data; successful recovery does not certify untouched main-file B-tree pages.

use std::path::PathBuf;

use fsqlite_error::Result;
use fsqlite_types::cx::Cx;

use crate::{Connection, ConnectionEnv, FileIdentity, FrankenError};
pub use fsqlite_wal::native_recovery::{ExportReport as WalRecoveryReport, Options as WalRecoveryOptions};

/// A certified repair and the independent outcome of opening its database.
///
/// An SQL open can fail after WAL repair has succeeded. Keeping its result
/// separate preserves the repair receipt and backup location instead of
/// misrepresenting an open error as a rolled-back repair. The new connection,
/// when present, is writable and retains the normal concurrent-writer defaults.
#[derive(Debug)]
#[must_use = "inspect both the repair receipt and the SQL open outcome"]
pub struct RepairedOpen {
    pub recovery: WalRecoveryReport,
    pub connection: Result<Connection>,
}

/// Repair an existing Unix WAL and open the same database with the default
/// connection environment.
///
/// Uses the caller's active native runtime and `Cx`;
/// no subprocess, executor, or second recovery algorithm is constructed.
///
/// See [`repair_and_open_with_env`] for failure and ownership semantics.
pub async fn repair_and_open(cx: &Cx, options: &WalRecoveryOptions) -> Result<RepairedOpen> {
    repair_and_open_with_env(cx, options, ConnectionEnv::default()).await
}

/// Repair this database, then open it for SQL using the supplied environment.
///
/// The recovery implementation validates all input, writes and verifies a new
/// backup, repairs the WAL, publishes its derived index, and restores recovery
/// locks before SQL initialization. Its original managed main-file descriptor
/// stays alive until the existing-only, expected-identity constructor returns.
/// A pathname replacement cannot redirect that constructor to another database
/// or cause a missing database to be created. Other cooperative writers may
/// commit after the recovery fence is restored; this is not a pinned snapshot.
///
/// Outer `Err` means repair or handoff was not certified. Source writes may
/// already have occurred; preserve the backup and reconcile before reuse.
/// Outer `Ok` contains a successful repair receipt even if `connection` is
/// `Err`, including cancellation observed between repair and SQL open. A
/// failed SQL open does not roll back repair, delete the backup, or trigger a
/// retry. Dropping the future can leave completed repair without an observed
/// receipt; it does not cancel already-running physical settlement.
///
/// Recovery cancellation uses `cx`. SQL execution uses the unchanged `env`,
/// including its context lineage and configuration. Callers that require a
/// shared cancellation lineage must supply a correspondingly configured env.
/// Paths are filesystem paths, not SQLite URIs. UTF-8 is required by the SQL
/// constructor; a non-UTF-8 requested source is rejected before recovery.
pub async fn repair_and_open_with_env(
    cx: &Cx,
    options: &WalRecoveryOptions,
    env: ConnectionEnv,
) -> Result<RepairedOpen> {
    if options.source.to_str().is_none() {
        return Err(FrankenError::CannotOpen { path: options.source.clone() });
    }
    let (connection, recovery) = options.repair_and_open(cx, |path, identity| {
        open_checked_source(path, identity, env)
    }).await?;
    Ok(RepairedOpen { recovery, connection })
}

async fn open_checked_source(
    path: PathBuf,
    identity: FileIdentity,
    env: ConnectionEnv,
) -> Result<Connection> {
    // A relative UTF-8 input may canonicalize through a non-UTF-8 parent.
    // Never silently lossy-convert it into a different filesystem pathname.
    let path = path.into_os_string().into_string()
        .map_err(|path| FrankenError::CannotOpen { path: path.into() })?;
    Connection::open_existing_with_expected_identity_and_env(path, identity, env).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_types::{ObjectId, Oti, SqliteValue};
    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_vfs::{UnixVfs, Vfs, VfsFile, host_fs};
    use fsqlite_wal::checksum::WalChecksumTransform;
    use fsqlite_wal::{
        SqliteWalChecksum, WAL_FORMAT_VERSION, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE,
        WAL_MAGIC_LE, WalFecGroupMeta, WalFecGroupMetaInit, WalFecGroupRecord,
        WalFrameHeader, WalHeader, WalSalts, append_wal_fec_group,
        build_source_page_hashes, generate_wal_fec_repair_symbols,
    };

    fn with_runtime<F: std::future::Future>(future: F) -> F::Output {
        asupersync::runtime::RuntimeBuilder::current_thread()
            .blocking_threads(1, 2).build().unwrap().block_on(future)
    }

    fn attached_context() -> Cx {
        let cx = Cx::new();
        cx.set_native_cx(asupersync::Cx::current().unwrap());
        cx
    }

    fn companion(path: &std::path::Path, suffix: &str) -> PathBuf {
        let mut name = path.as_os_str().to_owned();
        name.push(suffix);
        PathBuf::from(name)
    }

    /// Stock SQLite supplies real schema/table pages; a three-frame FEC group
    /// updates user_version without changing the table payload. The first
    /// payload is damaged after encoding, before FrankenSQLite sees the source.
    fn fixture() -> (WalRecoveryOptions, Vec<u8>) {
        let directory = tempfile::tempdir().unwrap().keep();
        let oracle_path = directory.join("oracle.db");
        let oracle = rusqlite::Connection::open(&oracle_path).unwrap();
        oracle.execute_batch(
            "PRAGMA page_size=512; CREATE TABLE sample(id INTEGER PRIMARY KEY, value TEXT); \
             INSERT INTO sample VALUES (1,'alpha'),(2,'beta'); PRAGMA user_version=0;",
        ).unwrap();
        oracle.close().unwrap();
        let mut database = host_fs::read(&oracle_path).unwrap();
        database[18..20].copy_from_slice(&[2, 2]);
        let db_size = u32::try_from(database.len() / 512).unwrap();
        let options = WalRecoveryOptions::new(directory.join("source.db"), directory.join("original.wal"));
        host_fs::write(&options.source, &database).unwrap();
        let pages: Vec<_> = (1_u32..=3).map(|version| {
            let mut page = database[..512].to_vec();
            page[60..64].copy_from_slice(&version.to_be_bytes());
            page
        }).collect();
        let header = WalHeader {
            magic: WAL_MAGIC_LE, format_version: WAL_FORMAT_VERSION, page_size: 512,
            checkpoint_seq: 1, salts: WalSalts { salt1: 123, salt2: 456 },
            checksum: SqliteWalChecksum::default(),
        };
        let mut wal = header.to_bytes().unwrap().to_vec();
        let mut running = WalHeader::from_bytes(&wal).unwrap().checksum;
        for (index, page) in pages.iter().enumerate() {
            let start = wal.len();
            wal.extend_from_slice(&WalFrameHeader {
                page_number: 1, db_size: if index == 2 { db_size } else { 0 },
                salts: header.salts, checksum: SqliteWalChecksum::default(),
            }.to_bytes());
            wal.extend_from_slice(page);
            running = WalChecksumTransform::for_wal_frame(&wal[start..], 512, false)
                .unwrap().apply(running);
            wal[start + 16..start + 20].copy_from_slice(&running.s1.to_be_bytes());
            wal[start + 20..start + 24].copy_from_slice(&running.s2.to_be_bytes());
        }
        let meta = WalFecGroupMeta::from_init(WalFecGroupMetaInit {
            wal_salt1: 123, wal_salt2: 456, start_frame_no: 1, end_frame_no: 3,
            db_size_pages: db_size, page_size: 512, k_source: 3, r_repair: 8,
            oti: Oti { f: 1536, al: 1, t: 512, z: 1, n: 1 },
            object_id: ObjectId::derive_from_canonical_bytes(b"sql-recovery-handoff"),
            page_numbers: vec![1; 3], source_page_xxh3_128: build_source_page_hashes(&pages),
        }).unwrap();
        let symbols = generate_wal_fec_repair_symbols(&meta, &pages).unwrap();
        append_wal_fec_group(&companion(&options.source, "-wal-fec"),
            &WalFecGroupRecord::new(meta, symbols).unwrap()).unwrap();
        wal[WAL_HEADER_SIZE + WAL_FRAME_HEADER_SIZE + 60] ^= 0xff;
        host_fs::write(&companion(&options.source, "-wal"), &wal).unwrap();
        (options, wal)
    }

    #[test]
    fn repaired_database_opens_for_sql_reads_and_writes_with_backup_receipt() {
        with_runtime(async {
            let (options, original_wal) = fixture();
            let cx = attached_context();
            let outcome = repair_and_open_with_env(&cx, &options, ConnectionEnv::default())
                .await.unwrap();
            assert_eq!(outcome.recovery.repaired_frames, 1);
            assert_eq!(outcome.recovery.wal_frames, 3);
            assert!(outcome.recovery.repaired_in_place);
            assert_eq!(host_fs::read(&outcome.recovery.destination).unwrap(), original_wal);
            let conn = outcome.connection.unwrap();
            assert_eq!(conn.query_row("PRAGMA user_version").await.unwrap().get(0),
                Some(&SqliteValue::Integer(3)));
            let rows = conn.query("SELECT id, value FROM sample ORDER BY id").await.unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].get(1), Some(&SqliteValue::Text("alpha".into())));
            assert_eq!(rows[1].get(1), Some(&SqliteValue::Text("beta".into())));
            conn.execute("INSERT INTO sample VALUES (3, 'gamma')").await.unwrap();
            assert_eq!(conn.query_row("SELECT count(*) FROM sample").await.unwrap().get(0),
                Some(&SqliteValue::Integer(3)));
            assert_eq!(conn.query_row("PRAGMA integrity_check").await.unwrap().get(0),
                Some(&SqliteValue::Text("ok".into())));
            conn.close().await.unwrap();
        });
    }

    #[test]
    fn existing_backup_refusal_preserves_the_damaged_source() {
        with_runtime(async {
            let (options, original_wal) = fixture();
            host_fs::write(&options.destination, b"existing backup").unwrap();
            let cx = attached_context();
            assert!(repair_and_open(&cx, &options).await.is_err());
            assert_eq!(host_fs::read(&options.destination).unwrap(), b"existing backup");
            assert_eq!(host_fs::read(&companion(&options.source, "-wal")).unwrap(), original_wal);
        });
    }

    #[test]
    fn pathname_replacement_between_repair_and_open_cannot_redirect_sql() {
        with_runtime(async {
            let (options, original_wal) = fixture();
            let cx = attached_context();
            let (connection, report) = options.repair_and_open(&cx, |source, identity| async move {
                let retired = source.with_file_name("original-main-retained.db");
                let replacement = source.with_file_name("replacement-retained.db");
                // Deliberately bypass cooperative namespace admission to model
                // an external replacement. Preserve both files, then restore
                // the original name before the identity owner is cleaned up.
                host_fs::rename(&source, &retired)?;
                host_fs::write(&source, b"replacement owner")?;
                let opened = open_checked_source(source.clone(), identity, ConnectionEnv::default()).await;
                assert!(opened.is_err(), "SQL must not follow the replacement");
                assert_eq!(host_fs::read(&source)?, b"replacement owner");
                host_fs::rename(&source, &replacement)?;
                host_fs::rename(&retired, &source)?;
                opened
            }).await.unwrap();
            assert!(connection.is_err());
            assert!(report.repaired_in_place);
            assert_eq!(report.repaired_frames, 1);
            assert_eq!(host_fs::read(&report.destination).unwrap(), original_wal);
        });
    }

    #[test]
    fn identity_bound_sql_ingress_rejects_replacement_and_never_creates_missing_file() {
        with_runtime(async {
            let (options, _) = fixture();
            let cx = attached_context();
            let vfs = UnixVfs::new();
            let (mut guard, _) = vfs.open(&cx, Some(&options.source),
                VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB).unwrap();
            let identity = guard.file_identity().unwrap().unwrap();
            let other = options.source.with_file_name("other.db");
            host_fs::write(&other, b"unrelated owner").unwrap();
            assert!(open_checked_source(other.clone(), identity, ConnectionEnv::default())
                .await.is_err());
            assert_eq!(host_fs::read(&other).unwrap(), b"unrelated owner");
            let missing = options.source.with_file_name("missing.db");
            assert!(open_checked_source(missing.clone(), identity, ConnectionEnv::default())
                .await.is_err());
            assert!(!vfs.path_entry_exists(&cx, &missing).unwrap());
            guard.close(&cx).unwrap();
        });
    }

    #[test]
    fn cancelled_recovery_does_not_start_sql_or_create_backup() {
        with_runtime(async {
            let (options, original_wal) = fixture();
            let cx = attached_context();
            cx.cancel();
            assert!(matches!(repair_and_open(&cx, &options).await, Err(FrankenError::Interrupt)));
            assert!(!options.destination.exists());
            assert_eq!(host_fs::read(&companion(&options.source, "-wal")).unwrap(), original_wal);
        });
    }

    #[test]
    fn non_utf8_requested_source_is_rejected_before_native_recovery() {
        use std::os::unix::ffi::OsStringExt;
        with_runtime(async {
            let directory = tempfile::tempdir().unwrap().keep();
            let source = directory.join(std::ffi::OsString::from_vec(b"source-\xff.db".to_vec()));
            host_fs::write(&source, b"untouched").unwrap();
            let options = WalRecoveryOptions::new(&source, directory.join("new-backup.wal"));
            let cx = attached_context();
            assert!(matches!(repair_and_open(&cx, &options).await, Err(FrankenError::CannotOpen { .. })));
            assert!(!options.destination.exists());
            assert_eq!(host_fs::read(&source).unwrap(), b"untouched");
        });
    }
}
