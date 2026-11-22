#![recursion_limit = "512"]

//! bd-dwjnq.5 / bd-dwjnq.6: Independent acceptance proof and stock-SQLite oracle
//! suite for `PRAGMA temp_store` governing live temporary allocation and spill.
//!
//! Verifies:
//! 1. Public `ORDER BY`, `GROUP BY`, `DISTINCT`, and `CREATE TEMP TABLE`
//!    workloads beyond a tiny `PRAGMA temp.cache_size` budget in `DEFAULT (0)`,
//!    `FILE (1)`, and `MEMORY (2)` modes produce results byte-for-byte identical
//!    to stock C SQLite (`rusqlite`), with genuine VFS temp file creation in
//!    `DEFAULT`/`FILE` modes, zero temp files in `MEMORY` mode, and zero
//!    residual files on disk after completion.
//! 2. Canonical `PRAGMA temp_store` mode transitions matching stock SQLite
//!    3.46.1: same-value no-op inside transactions, atomic refusal with state
//!    preservation when changing mode inside an active transaction or savepoint
//!    with existing TEMP objects, allowed change inside a main-only transaction
//!    with no TEMP objects, and atomic TEMP teardown + shadowed-main restoration
//!    + prepared-statement invalidation outside a transaction.
//! 3. Disk-full (`DatabaseFull`), mid-write I/O failure (`Io`), caller `Cx`
//!    cancellation (`Abort`), and `Connection::close_in_place` cleanup leave
//!    zero active handles and zero residual files.
//! 4. Nested statements (CTEs, `INSERT ... SELECT`, triggers into `TEMP`
//!    tables) and encrypted database (`PRAGMA key`) temp spills (zero plaintext
//!    bytes ever touch disk even under `PRAGMA temp_store = FILE`).
//! 5. Planted negative control proving that a stored-only / file-disabled
//!    policy fails the live spill acceptance predicate.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use asupersync::test_utils::run_test;
use fsqlite_core::connection::{Connection, ConnectionEnv, RuntimeConfig};
use fsqlite_error::{ErrorCode, FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::VfsOpenFlags;
use fsqlite_types::value::SqliteValue;
use fsqlite_vdbe::temp_store::{
    EffectiveTempStore, TempFileBackend, TempFileHandle, TempStoreMode, TempStoreRuntime,
    TempStoreStats, map_io_error_for_temp_store,
};
use fsqlite_vfs::traits::{Vfs, VfsFile};
#[cfg(unix)]
use fsqlite_vfs::{UnixFile as NativeFile, UnixVfs as NativeVfs};
#[cfg(windows)]
use fsqlite_vfs::{WindowsFile as NativeFile, WindowsVfs as NativeVfs};
use tempfile::TempDir;

fn sqlite_val_from_rusqlite(val: rusqlite::types::Value) -> SqliteValue {
    match val {
        rusqlite::types::Value::Null => SqliteValue::Null,
        rusqlite::types::Value::Integer(i) => SqliteValue::Integer(i),
        rusqlite::types::Value::Real(f) => SqliteValue::Float(f),
        rusqlite::types::Value::Text(s) => SqliteValue::Text(s.into()),
        rusqlite::types::Value::Blob(b) => SqliteValue::Blob(b.into()),
    }
}

fn rusqlite_query(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    let mut stmt = conn
        .prepare(sql)
        .unwrap_or_else(|e| panic!("rusqlite prepare `{sql}`: {e}"));
    let col_count = stmt.column_count();
    let rows = stmt
        .query_map([], |row| {
            let mut vals = Vec::with_capacity(col_count);
            for i in 0..col_count {
                let raw: rusqlite::types::Value = row.get(i)?;
                vals.push(sqlite_val_from_rusqlite(raw));
            }
            Ok(vals)
        })
        .unwrap_or_else(|e| panic!("rusqlite query `{sql}`: {e}"));
    rows.map(|r| r.expect("rusqlite row")).collect()
}

async fn fsqlite_query(conn: &Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("fsqlite query `{sql}`: {e:?}"))
        .into_iter()
        .map(|row| row.values().to_vec())
        .collect()
}

fn count_dir_entries(dir: &Path) -> Vec<PathBuf> {
    fs::read_dir(dir)
        .expect("read temp dir")
        .map(|entry| entry.expect("dir entry").path())
        .collect()
}

#[test]
fn synchronous_spill_io_preserves_offset_eof_and_cancellation() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("offset.tmp");
    let cx = Cx::new();
    let flags = VfsOpenFlags::TEMP_DB | VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE;
    let (mut file, _) = NativeVfs::new().open(&cx, Some(&path), flags).unwrap();
    let payload = vec![0xa5; 65_537];
    file.write_at_sync(&cx, &payload, 7).unwrap();
    let mut actual = vec![0xff; payload.len() + 16];
    assert_eq!(
        file.read_at_sync(&cx, &mut actual, 0).unwrap(),
        7 + payload.len()
    );
    assert_eq!(&actual[..7], &[0; 7]);
    assert_eq!(&actual[7..7 + payload.len()], payload);
    assert_eq!(&actual[7 + payload.len()..], &[0; 9]);
    let cancelled = Cx::new();
    cancelled.cancel();
    assert!(matches!(
        file.write_at_sync(&cancelled, &[0], 7),
        Err(FrankenError::Abort)
    ));
    assert!(matches!(
        file.read_at_sync(&cancelled, &mut actual, 7),
        Err(FrankenError::Abort)
    ));
    file.close(&cx).unwrap();
    assert_eq!(&fs::read(path).unwrap()[7..], payload);
}

#[test]
fn large_sorter_spill_builds_a_stock_readable_index() {
    const SETUP: &str = "PRAGMA temp_store=FILE; PRAGMA temp.cache_size=1;
        CREATE TABLE s(id INTEGER PRIMARY KEY, v BLOB);
        INSERT INTO s VALUES(1,zeroblob(1048577)),(2,x'11'),(3,x'01');";
    const INDEX: &str = "CREATE INDEX s_v ON s(v)";
    const READ: &str = "SELECT id,length(v) FROM s INDEXED BY s_v ORDER BY v";
    run_test(|| async {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("large-spill.db");
        let conn = Connection::open(path.to_str().unwrap()).await.unwrap();
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(SETUP).await.unwrap();
        stock.execute_batch(SETUP).unwrap();
        let before = conn.temp_store_stats();
        stock.execute_batch(INDEX).unwrap();
        conn.execute(INDEX).await.unwrap();
        let after = conn.temp_store_stats();
        assert!(after.sorter_spill_runs > before.sorter_spill_runs);
        assert!(after.sorter_spill_bytes - before.sorter_spill_bytes > 1_048_576);
        assert_eq!(after.active_temp_files, 0);
        let expected = rusqlite_query(&stock, READ);
        assert_eq!(expected.len(), 3);
        assert_eq!(fsqlite_query(&conn, READ).await, expected);
        conn.close().await.unwrap();
        let reopened = rusqlite::Connection::open(&path).unwrap();
        assert_eq!(rusqlite_query(&reopened, READ), expected);
        assert_eq!(
            reopened
                .query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
    });
}

/// Inspecting native VFS backend that records every physical file created in
/// `dir`, verifies the file exists on disk while open, captures raw bytes
/// written (for optional zero-plaintext inspection), and can inject disk-full
/// or I/O write faults deterministically.
struct ObservingVfsTempBackend {
    dir: PathBuf,
    files_created: AtomicU64,
    files_seen_on_disk: AtomicU64,
    attempted_write_bytes: Arc<AtomicU64>,
    fail_after_bytes: Option<u64>,
    fail_with_disk_full: bool,
    capture_raw_bytes: bool,
    captured_bytes: Arc<Mutex<Vec<u8>>>,
}

impl ObservingVfsTempBackend {
    fn new(dir: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            dir,
            files_created: AtomicU64::new(0),
            files_seen_on_disk: AtomicU64::new(0),
            attempted_write_bytes: Arc::new(AtomicU64::new(0)),
            fail_after_bytes: None,
            fail_with_disk_full: false,
            capture_raw_bytes: false,
            captured_bytes: Arc::new(Mutex::new(Vec::new())),
        })
    }

    fn with_fault(dir: PathBuf, fail_after_bytes: u64, disk_full: bool) -> Arc<Self> {
        Arc::new(Self {
            dir,
            files_created: AtomicU64::new(0),
            files_seen_on_disk: AtomicU64::new(0),
            attempted_write_bytes: Arc::new(AtomicU64::new(0)),
            fail_after_bytes: Some(fail_after_bytes),
            fail_with_disk_full: disk_full,
            capture_raw_bytes: false,
            captured_bytes: Arc::new(Mutex::new(Vec::new())),
        })
    }
}

struct ObservingVfsTempHandle {
    file: Mutex<Option<NativeFile>>,
    attempted_write_bytes: Arc<AtomicU64>,
    fail_after_bytes: Option<u64>,
    fail_with_disk_full: bool,
    capture_raw_bytes: bool,
    captured_bytes: Arc<Mutex<Vec<u8>>>,
}

impl TempFileHandle for ObservingVfsTempHandle {
    fn write_at(&self, cx: &Cx, offset: u64, data: &[u8]) -> Result<()> {
        if cx.is_cancel_requested() {
            return Err(FrankenError::Abort);
        }
        let next = self
            .attempted_write_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed)
            .saturating_add(data.len() as u64);
        if let Some(limit) = self.fail_after_bytes
            && next >= limit
        {
            return if self.fail_with_disk_full {
                #[cfg(unix)]
                let disk_full = std::io::Error::from_raw_os_error(nix::libc::ENOSPC);
                #[cfg(windows)]
                let disk_full = std::io::Error::from_raw_os_error(112); // ERROR_DISK_FULL
                Err(map_io_error_for_temp_store(disk_full))
            } else {
                Err(FrankenError::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "injected temp VFS write failure",
                )))
            };
        }
        if self.capture_raw_bytes {
            self.captured_bytes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend_from_slice(data);
        }
        let mut guard = self
            .file
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(file) = guard.as_mut() else {
            return Err(FrankenError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "observing temp file already closed",
            )));
        };
        file.write_at_sync(cx, data, offset)
    }

    fn read_at(&self, cx: &Cx, offset: u64, buf: &mut [u8]) -> Result<usize> {
        if cx.is_cancel_requested() {
            return Err(FrankenError::Abort);
        }
        let guard = self
            .file
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(file) = guard.as_ref() else {
            return Err(FrankenError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "observing temp file already closed",
            )));
        };
        file.read_at_sync(cx, buf, offset)
    }

    fn file_size(&self, cx: &Cx) -> Result<u64> {
        let guard = self
            .file
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(file) = guard.as_ref() else {
            return Ok(0);
        };
        file.file_size(cx)
    }

    fn truncate(&self, cx: &Cx, size: u64) -> Result<()> {
        let mut guard = self
            .file
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(file) = guard.as_mut() else {
            return Ok(());
        };
        file.truncate(cx, size)
    }

    fn close(&self, cx: &Cx) -> Result<()> {
        let mut guard = self
            .file
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(mut file) = guard.take() {
            return file.close(cx);
        }
        Ok(())
    }
}

impl Drop for ObservingVfsTempHandle {
    fn drop(&mut self) {
        let mut guard = self
            .file
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(mut file) = guard.take() {
            let cx = Cx::new();
            let _ = file.close(&cx);
        }
    }
}

impl TempFileBackend for ObservingVfsTempBackend {
    fn create_temp_file(&self, cx: &Cx, purpose: &str) -> Result<Box<dyn TempFileHandle>> {
        if cx.is_cancel_requested() {
            return Err(FrankenError::Abort);
        }
        let vfs = NativeVfs::new();
        let seq = self.files_created.fetch_add(1, Ordering::Relaxed);
        let mut rng = [0u8; 8];
        vfs.randomness(cx, &mut rng);
        let hex: String = rng.iter().map(|b| format!("{b:02x}")).collect();
        let path = self
            .dir
            .join(format!("obs_temp_{purpose}_{seq}_{hex}.tmp"));
        let flags = VfsOpenFlags::TEMP_DB
            | VfsOpenFlags::READWRITE
            | VfsOpenFlags::CREATE
            | VfsOpenFlags::EXCLUSIVE
            | VfsOpenFlags::DELETEONCLOSE;
        let (file, _) = vfs.open(cx, Some(&path), flags)?;
        if path.exists() {
            self.files_seen_on_disk.fetch_add(1, Ordering::Relaxed);
        }
        Ok(Box::new(ObservingVfsTempHandle {
            file: Mutex::new(Some(file)),
            attempted_write_bytes: Arc::clone(&self.attempted_write_bytes),
            fail_after_bytes: self.fail_after_bytes,
            fail_with_disk_full: self.fail_with_disk_full,
            capture_raw_bytes: self.capture_raw_bytes,
            captured_bytes: Arc::clone(&self.captured_bytes),
        }))
    }
}

#[test]
fn disk_full_fault_budget_spans_separate_spill_files() {
    let dir = TempDir::new().unwrap();
    let backend = ObservingVfsTempBackend::with_fault(dir.path().to_owned(), 8, true);
    let cx = Cx::new();
    let first = backend.create_temp_file(&cx, "first").unwrap();
    let second = backend.create_temp_file(&cx, "second").unwrap();
    first.write_at(&cx, 0, &[1; 4]).unwrap();
    second.write_at(&cx, 0, &[2; 3]).unwrap();
    assert!(matches!(
        second.write_at(&cx, 3, &[3]),
        Err(FrankenError::DatabaseFull)
    ));
    assert_eq!(first.file_size(&cx).unwrap(), 4);
    assert_eq!(second.file_size(&cx).unwrap(), 3);
    first.close(&cx).unwrap();
    second.close(&cx).unwrap();
    assert!(count_dir_entries(dir.path()).is_empty());
}

/// Negative-control backend representing an inert / file-incapable runtime.
struct StoredOnlyInertBackend;

impl TempFileBackend for StoredOnlyInertBackend {
    fn create_temp_file(&self, _cx: &Cx, _purpose: &str) -> Result<Box<dyn TempFileHandle>> {
        Err(FrankenError::Unsupported)
    }

    fn supports_file_storage(&self) -> bool {
        false
    }
}

#[test]
fn test_temp_store_file_default_memory_sorts_groups_temp_tables_vs_stock_oracle() {
    run_test(|| async {
        let dir = TempDir::new().expect("tempdir");
        let db_path = dir.path().join("oracle.db");
        let temp_dir = TempDir::new().expect("spill temp dir");

        // Build identical baseline datasets in FrankenSQLite and stock C SQLite.
        let conn = Connection::open(db_path.to_str().unwrap())
            .await
            .expect("open fsqlite");
        assert!(
            conn.is_concurrent_mode_default(),
            "concurrent-writer default must stay ON"
        );
        let stock = rusqlite::Connection::open_in_memory().expect("open rusqlite");

        let setup_sql = [
            "CREATE TABLE items(id INTEGER PRIMARY KEY, grp TEXT, tag TEXT, score INTEGER, payload TEXT);",
        ];
        for sql in setup_sql {
            conn.execute(sql).await.expect("fsqlite ddl");
            stock.execute(sql, []).expect("rusqlite ddl");
        }

        conn.execute("BEGIN;").await.expect("begin");
        stock.execute("BEGIN;", []).expect("stock begin");
        for i in 1..=160_i64 {
            let grp = match i % 7 {
                0 => "NULL".to_owned(),
                1 => "'Alpha'".to_owned(),
                2 => "'alpha'".to_owned(),
                3 => "'Beta  '".to_owned(),
                4 => "'Beta'".to_owned(),
                5 => "'gamma'".to_owned(),
                _ => "'Delta'".to_owned(),
            };
            let tag = if i % 11 == 0 {
                "NULL".to_owned()
            } else {
                format!("'T{:03}'", (i * 37) % 29)
            };
            let score = if i % 13 == 0 {
                "NULL".to_owned()
            } else {
                format!("{}", (i * 19) % 41 - 20)
            };
            let payload = format!("'row-{i:04}-{}'", "P".repeat(96));
            let sql = format!("INSERT INTO items VALUES ({i}, {grp}, {tag}, {score}, {payload});");
            conn.execute(&sql).await.expect("fsqlite insert");
            stock.execute(&sql, []).expect("rusqlite insert");
        }
        conn.execute("COMMIT;").await.expect("commit");
        stock.execute("COMMIT;", []).expect("stock commit");

        // Configure tiny 4 KiB temp budget via standard SQLite PRAGMA temp.cache_size
        // and isolate temp VFS files inside `temp_dir`.
        let temp_dir_str = temp_dir.path().to_str().unwrap();
        conn.execute(&format!("PRAGMA temp_store_directory = '{temp_dir_str}';"))
            .await
            .expect("set temp_store_directory");
        let dir_readback = fsqlite_query(&conn, "PRAGMA temp_store_directory;").await;
        assert_eq!(
            dir_readback,
            vec![vec![SqliteValue::Text(temp_dir_str.into())]]
        );
        conn.execute("PRAGMA temp.cache_size = -4;")
            .await
            .expect("set tiny 4 KiB temp budget");

        let queries = [
            // Multi-column ORDER BY with NULLS LAST / NULLS FIRST + NOCASE collation
            "SELECT id, grp, score FROM items ORDER BY grp COLLATE NOCASE ASC NULLS LAST, score DESC NULLS FIRST, id ASC;",
            // RTRIM collation + DESC order
            "SELECT id, grp, tag FROM items ORDER BY grp COLLATE RTRIM DESC, tag ASC NULLS LAST, id ASC;",
            // GROUP BY with NULL group + HAVING + aggregates
            "SELECT grp, COUNT(*), SUM(score), MIN(id), MAX(id) FROM items GROUP BY grp HAVING COUNT(*) >= 5 ORDER BY grp ASC NULLS FIRST;",
            // DISTINCT (exercises ephemeral autoindex / sorter)
            "SELECT DISTINCT grp, tag FROM items ORDER BY grp ASC NULLS FIRST, tag ASC NULLS FIRST;",
        ];

        // Mode A: DEFAULT (0) -> file-backed on native; Mode B: FILE (1) -> file-backed; Mode C: MEMORY (2) -> pure memory.
        for (mode_sql, expected_code, expect_file_spill) in [
            ("PRAGMA temp_store = DEFAULT;", 0_i64, true),
            ("PRAGMA temp_store = FILE;", 1_i64, true),
            ("PRAGMA temp_store = MEMORY;", 2_i64, false),
        ] {
            let assign_rows = fsqlite_query(&conn, mode_sql).await;
            assert!(
                assign_rows.is_empty(),
                "`{mode_sql}` assignment must emit 0 rows, got {assign_rows:?}"
            );
            assert_eq!(
                fsqlite_query(&conn, "PRAGMA temp_store;").await,
                vec![vec![SqliteValue::Integer(expected_code)]]
            );

            let observer = ObservingVfsTempBackend::new(temp_dir.path().to_path_buf());
            conn.set_temp_file_backend(Some(Arc::clone(&observer) as Arc<dyn TempFileBackend>));
            let before = conn.temp_store_stats();
            assert_eq!(before.configured_mode, expected_code);
            assert_eq!(before.effective_mode_is_file, expect_file_spill);
            assert_eq!(before.budget_bytes, 4 * 1024);

            for sql in queries {
                let actual = fsqlite_query(&conn, sql).await;
                let expected = rusqlite_query(&stock, sql);
                assert_eq!(
                    actual, expected,
                    "mode {mode_sql} diverged from stock SQLite on `{sql}`"
                );
            }

            // Exercise CREATE TEMP TABLE + TEMP INDEX + UPDATE + DELETE + ORDER BY beyond 4 KiB budget.
            let temp_ddl = [
                "CREATE TEMP TABLE work_tmp(k INTEGER PRIMARY KEY, g TEXT, body TEXT);",
                "CREATE INDEX temp.idx_work_tmp_g ON work_tmp(g COLLATE NOCASE);",
                "INSERT INTO work_tmp SELECT id, grp, payload FROM items;",
                "UPDATE work_tmp SET body = body || '-upd' WHERE k % 10 = 0;",
                "DELETE FROM work_tmp WHERE k % 15 = 0;",
            ];
            for sql in temp_ddl {
                conn.execute(sql).await.expect("fsqlite temp ddl/dml");
                stock.execute(sql, []).expect("rusqlite temp ddl/dml");
            }
            let temp_check_sql =
                "SELECT k, g, length(body) FROM work_tmp ORDER BY g COLLATE NOCASE ASC NULLS LAST, k DESC LIMIT 40;";
            assert_eq!(
                fsqlite_query(&conn, temp_check_sql).await,
                rusqlite_query(&stock, temp_check_sql),
                "mode {mode_sql} TEMP table results diverged from stock SQLite"
            );
            conn.execute("DROP TABLE work_tmp;")
                .await
                .expect("drop fsqlite temp table");
            stock
                .execute("DROP TABLE work_tmp;", [])
                .expect("drop stock temp table");

            let after = conn.temp_store_stats();
            if expect_file_spill {
                assert!(
                    after.sorter_spill_runs > before.sorter_spill_runs,
                    "mode {mode_sql} must spill sorter runs to VFS temp files: before={before:?} after={after:?}"
                );
                assert!(
                    after.temp_table_spill_events > before.temp_table_spill_events,
                    "mode {mode_sql} must spill TEMP table rows to VFS temp files: before={before:?} after={after:?}"
                );
                assert!(
                    observer.files_created.load(Ordering::Relaxed) >= 2,
                    "mode {mode_sql} must open real VFS temp files"
                );
                assert!(
                    observer.files_seen_on_disk.load(Ordering::Relaxed) >= 2,
                    "mode {mode_sql} temp files must physically exist on disk while open"
                );
            } else {
                assert_eq!(
                    after.sorter_spill_runs, before.sorter_spill_runs,
                    "MEMORY mode must not spill sorter runs"
                );
                assert_eq!(
                    after.temp_table_spill_events, before.temp_table_spill_events,
                    "MEMORY mode must not spill TEMP tables"
                );
                assert_eq!(
                    after.temp_index_spill_events, before.temp_index_spill_events,
                    "MEMORY mode must not spill ephemeral indexes"
                );
                assert_eq!(
                    observer.files_created.load(Ordering::Relaxed),
                    0,
                    "MEMORY mode must create zero temp files"
                );
                assert!(
                    after.memory_only_allocations > before.memory_only_allocations,
                    "MEMORY mode must record memory-only allocations"
                );
            }
            assert_eq!(
                after.active_temp_files, 0,
                "mode {mode_sql} leaked active temp files: {after:?}"
            );
            assert_eq!(after.temp_files_opened, after.temp_files_closed);
            assert!(
                count_dir_entries(temp_dir.path()).is_empty(),
                "mode {mode_sql} left residual files in temp dir: {:?}",
                count_dir_entries(temp_dir.path())
            );
        }

        conn.close().await.expect("close");
    });
}

#[test]
fn test_temp_store_transitions_existing_objects_transactions_prepared_statements_vs_stock_oracle() {
    run_test(|| async {
        let temp_dir = TempDir::new().expect("temp dir");
        let conn = Connection::open(":memory:").await.expect("open fsqlite");
        let stock = rusqlite::Connection::open_in_memory().expect("open rusqlite");
        conn.set_temp_store_directory(Some(temp_dir.path().to_path_buf()))
            .expect("set temp dir");
        conn.execute("PRAGMA temp.cache_size = -4;")
            .await
            .expect("tiny temp budget");

        // Create a main table that will be shadowed by a same-named TEMP table.
        let init_sql = [
            "CREATE TABLE shared(id INTEGER PRIMARY KEY, src TEXT);",
            "INSERT INTO shared VALUES (1, 'main-row');",
            "CREATE TEMP TABLE shared(id INTEGER PRIMARY KEY, src TEXT);",
            "CREATE INDEX temp.idx_shared_src ON shared(src);",
            "CREATE TEMP VIEW v_shared AS SELECT id, src FROM shared;",
            "CREATE TEMP TABLE audit_log(msg TEXT);",
            "CREATE TEMP TRIGGER tr_shared AFTER INSERT ON shared BEGIN INSERT INTO audit_log VALUES (NEW.src); END;",
        ];
        for sql in init_sql {
            conn.execute(sql).await.expect("fsqlite init");
            stock.execute(sql, []).expect("rusqlite init");
        }
        // Populate TEMP table past 4 KiB so it spills to a real VFS temp file.
        for i in 1..=60_i64 {
            let sql = format!(
                "INSERT INTO shared VALUES ({i}, 'temp-row-{i:03}-{}');",
                "Q".repeat(96)
            );
            conn.execute(&sql).await.expect("fsqlite insert temp");
            stock.execute(&sql, []).expect("rusqlite insert temp");
        }
        assert!(
            conn.temp_store_stats().active_temp_files >= 1,
            "spilled TEMP table should hold an open VFS temp file before transition"
        );

        // 1. Inside BEGIN transaction: same-value `PRAGMA temp_store = DEFAULT` succeeds & preserves state.
        conn.execute("BEGIN;").await.expect("begin");
        stock.execute("BEGIN;", []).expect("stock begin");
        conn.execute("PRAGMA temp_store = DEFAULT;")
            .await
            .expect("same-value inside txn succeeds");
        stock
            .execute("PRAGMA temp_store = DEFAULT;", [])
            .expect("stock same-value inside txn succeeds");

        // 2. Inside BEGIN transaction: changing `PRAGMA temp_store = MEMORY` with existing TEMP objects
        //    MUST fail atomically with stock's exact error, preserving temp_store=0 and all TEMP objects!
        let f_err = conn
            .execute("PRAGMA temp_store = MEMORY;")
            .await
            .expect_err("mode change inside txn with temp objects must fail");
        let s_err = stock
            .execute("PRAGMA temp_store = MEMORY;", [])
            .expect_err("stock mode change inside txn with temp objects must fail");
        assert!(
            f_err
                .to_string()
                .contains("temporary storage cannot be changed from within a transaction"),
            "unexpected fsqlite error: {f_err:?} (stock={s_err:?})"
        );
        assert_eq!(f_err.error_code(), ErrorCode::Error);
        assert_eq!(
            fsqlite_query(&conn, "PRAGMA temp_store;").await,
            rusqlite_query(&stock, "PRAGMA temp_store;"),
            "failed transition must preserve old temp_store value"
        );
        assert_eq!(
            fsqlite_query(&conn, "SELECT COUNT(*) FROM shared;").await,
            rusqlite_query(&stock, "SELECT COUNT(*) FROM shared;"),
            "failed transition must preserve TEMP table rows"
        );
        assert_eq!(
            fsqlite_query(&conn, "SELECT COUNT(*) FROM audit_log;").await,
            rusqlite_query(&stock, "SELECT COUNT(*) FROM audit_log;"),
            "failed transition must preserve TEMP trigger effects"
        );
        conn.execute("COMMIT;").await.expect("commit");
        stock.execute("COMMIT;", []).expect("stock commit");

        // 3. Inside SAVEPOINT with existing TEMP objects: changing mode MUST also fail atomically!
        conn.execute("SAVEPOINT sp1;").await.expect("savepoint");
        let sp_err = conn
            .execute("PRAGMA temp_store = MEMORY;")
            .await
            .expect_err("mode change inside savepoint with temp objects must fail");
        assert!(
            sp_err
                .to_string()
                .contains("temporary storage cannot be changed from within a transaction")
        );
        assert_eq!(
            fsqlite_query(&conn, "PRAGMA temp_store;").await,
            vec![vec![SqliteValue::Integer(0)]]
        );
        conn.execute("RELEASE sp1;").await.expect("release");

        // 4. Prepare a statement referencing the TEMP view `v_shared` BEFORE mode change.
        let prepared_temp_view = conn
            .prepare("SELECT COUNT(*) FROM v_shared;")
            .await
            .expect("prepare temp view query");
        assert_eq!(
            prepared_temp_view
                .query()
                .await
                .expect("pre-transition prepared query")[0]
                .values(),
            &[SqliteValue::Integer(60)]
        );

        // 5. Outside any transaction: changing `PRAGMA temp_store = MEMORY` atomically drops ALL
        //    TEMP tables/indexes/views/triggers, closes & unlinks VFS temp files, restores the
        //    shadowed `main.shared` table, and invalidates the stale prepared statement!
        conn.execute("PRAGMA temp_store = MEMORY;")
            .await
            .expect("autocommit mode change succeeds");
        stock
            .execute("PRAGMA temp_store = MEMORY;", [])
            .expect("stock autocommit mode change succeeds");
        assert_eq!(
            fsqlite_query(&conn, "PRAGMA temp_store;").await,
            rusqlite_query(&stock, "PRAGMA temp_store;")
        );
        // Shadowed main table `shared` is restored!
        assert_eq!(
            fsqlite_query(&conn, "SELECT id, src FROM shared;").await,
            rusqlite_query(&stock, "SELECT id, src FROM shared;"),
            "shadowed main table must become visible again after TEMP teardown"
        );
        // Previously-prepared statement referencing dropped TEMP view must now fail cleanly.
        let stale_err = prepared_temp_view
            .query()
            .await
            .expect_err("prepared statement on dropped TEMP view must fail after mode transition");
        assert!(
            matches!(stale_err, FrankenError::NoSuchTable { .. }),
            "expected NoSuchTable for dropped TEMP view, got {stale_err:?}"
        );
        // All VFS temp files from the dropped TEMP tables must be closed and unlinked.
        let stats = conn.temp_store_stats();
        assert_eq!(stats.active_temp_files, 0);
        assert_eq!(stats.temp_files_opened, stats.temp_files_closed);
        assert!(count_dir_entries(temp_dir.path()).is_empty());

        // 6a. Immediately after TEMP teardown (before any TEMP query re-initializes
        //     `db->aDb[1]`), changing mode inside a main-only transaction succeeds (stock parity!).
        conn.execute("BEGIN;").await.expect("begin main only");
        stock.execute("BEGIN;", []).expect("stock begin main only");
        conn.execute("INSERT INTO shared VALUES (2, 'main-2');")
            .await
            .expect("insert main");
        stock
            .execute("INSERT INTO shared VALUES (2, 'main-2');", [])
            .expect("stock insert main");
        conn.execute("PRAGMA temp_store = FILE;")
            .await
            .expect("mode change inside main txn with uninitialized temp db succeeds");
        stock
            .execute("PRAGMA temp_store = FILE;", [])
            .expect("stock mode change inside main txn with uninitialized temp db succeeds");
        assert_eq!(
            fsqlite_query(&conn, "PRAGMA temp_store;").await,
            rusqlite_query(&stock, "PRAGMA temp_store;")
        );
        conn.execute("COMMIT;").await.expect("commit");
        stock.execute("COMMIT;", []).expect("stock commit");

        // 6b. Querying `sqlite_temp_master` confirms all TEMP objects were dropped AND
        //     lazily re-initializes the TEMP handle (`db->aDb[1]`), so a subsequent mode
        //     change inside `BEGIN` fails identically to stock SQLite!
        assert_eq!(
            fsqlite_query(&conn, "SELECT type, name FROM sqlite_temp_master;").await,
            rusqlite_query(&stock, "SELECT type, name FROM sqlite_temp_master;"),
            "sqlite_temp_master must be empty after mode transition"
        );
        conn.execute("BEGIN;").await.expect("begin after temp_master");
        stock
            .execute("BEGIN;", [])
            .expect("stock begin after temp_master");
        let f_reinit_err = conn
            .execute("PRAGMA temp_store = MEMORY;")
            .await
            .expect_err("mode change inside txn after sqlite_temp_master query must fail");
        let s_reinit_err = stock
            .execute("PRAGMA temp_store = MEMORY;", [])
            .expect_err("stock mode change inside txn after sqlite_temp_master query must fail");
        assert!(
            f_reinit_err
                .to_string()
                .contains("temporary storage cannot be changed from within a transaction"),
            "unexpected fsqlite error after sqlite_temp_master: {f_reinit_err:?} (stock={s_reinit_err:?})"
        );
        conn.execute("COMMIT;").await.expect("commit after reinit");
        stock
            .execute("COMMIT;", [])
            .expect("stock commit after reinit");

        conn.close().await.expect("close");
    });
}

#[test]
fn test_temp_store_disk_full_write_failure_cancellation_and_close_cleanup() {
    run_test(|| async {
        let dir = TempDir::new().expect("db dir");
        let temp_dir = TempDir::new().expect("spill dir");
        let db_path = dir.path().join("faults.db");

        // Setup data in a file-backed DB.
        let setup_conn = Connection::open(db_path.to_str().unwrap())
            .await
            .expect("open setup");
        setup_conn
            .execute("CREATE TABLE big(id INTEGER PRIMARY KEY, body TEXT);")
            .await
            .expect("create big");
        setup_conn.execute("BEGIN;").await.expect("begin");
        for i in 1..=120_i64 {
            let sql = format!("INSERT INTO big VALUES ({i}, 'payload-{i:04}-{}');", "X".repeat(120));
            setup_conn.execute(&sql).await.expect("insert");
        }
        setup_conn.execute("COMMIT;").await.expect("commit");
        setup_conn.close().await.expect("close setup");

        // 1. Disk-full (ENOSPC -> FrankenError::DatabaseFull) and write I/O error (FrankenError::Io).
        for (label, disk_full) in [("disk_full", true), ("io_write_err", false)] {
            let conn = Connection::open(db_path.to_str().unwrap())
                .await
                .expect("open fault conn");
            conn.execute("PRAGMA temp_store = FILE;").await.expect("file mode");
            conn.execute("PRAGMA temp.cache_size = -2;")
                .await
                .expect("2 KiB budget");
            let fault_backend = ObservingVfsTempBackend::with_fault(
                temp_dir.path().to_path_buf(),
                128,
                disk_full,
            );
            conn.set_temp_file_backend(Some(fault_backend));

            // Sorter spill fault.
            let sort_err = conn
                .query("SELECT id, body FROM big ORDER BY body DESC, id ASC;")
                .await
                .expect_err("faulty temp backend must fail spilled sort");
            if disk_full {
                assert!(
                    matches!(sort_err, FrankenError::DatabaseFull),
                    "{label}: expected DatabaseFull from sort spill, got {sort_err:?}"
                );
            } else {
                assert!(
                    matches!(sort_err, FrankenError::Io(_)),
                    "{label}: expected Io from sort spill, got {sort_err:?}"
                );
            }
            assert_eq!(
                conn.temp_store_stats().active_temp_files,
                0,
                "{label}: sort fault leaked active temp files"
            );
            assert!(
                count_dir_entries(temp_dir.path()).is_empty(),
                "{label}: sort fault left residual files"
            );

            // TEMP table spill fault.
            conn.execute("CREATE TEMP TABLE fault_tmp(id INTEGER PRIMARY KEY, body TEXT);")
                .await
                .expect("create fault_tmp");
            let tbl_err = conn
                .execute("INSERT INTO fault_tmp SELECT id, body FROM big;")
                .await
                .expect_err("faulty temp backend must fail spilled temp table insert");
            if disk_full {
                assert!(
                    matches!(tbl_err, FrankenError::DatabaseFull),
                    "{label}: expected DatabaseFull from temp table spill, got {tbl_err:?}"
                );
            } else {
                assert!(
                    matches!(tbl_err, FrankenError::Io(_)),
                    "{label}: expected Io from temp table spill, got {tbl_err:?}"
                );
            }
            // Statement-level atomicity unwinds the partial insert on failure.
            assert_eq!(
                fsqlite_query(&conn, "SELECT COUNT(*) FROM fault_tmp;").await,
                vec![vec![SqliteValue::Integer(0)]],
                "{label}: failed INSERT INTO temp table must unwind atomically"
            );
            conn.close().await.expect("close fault conn");
            assert!(
                count_dir_entries(temp_dir.path()).is_empty(),
                "{label}: close left residual files"
            );
        }

        // 2. Caller-Cx cancellation aborts cleanly and releases all temp files.
        {
            let root_cx = Cx::new();
            let env = ConnectionEnv::new_with_root_cx(RuntimeConfig::default(), &root_cx);
            let conn = Connection::open_with_env(db_path.to_str().unwrap(), env)
                .await
                .expect("open cancel conn");
            conn.execute("PRAGMA temp_store = FILE;").await.expect("file mode");
            conn.execute("PRAGMA temp.cache_size = -2;")
                .await
                .expect("2 KiB budget");
            conn.set_temp_store_directory(Some(temp_dir.path().to_path_buf()))
                .expect("set temp dir");
            root_cx.cancel();
            let cancel_err = conn
                .query("SELECT id, body FROM big ORDER BY body DESC;")
                .await
                .expect_err("cancelled cx must abort spilled query");
            assert!(
                matches!(cancel_err, FrankenError::Abort),
                "expected Abort on cancelled cx, got {cancel_err:?}"
            );
            assert_eq!(conn.temp_store_stats().active_temp_files, 0);
            assert!(count_dir_entries(temp_dir.path()).is_empty());
            // The connection's root Cx is cancelled, so close() cleanly returns Abort
            // or Ok without leaking any temp handles or files.
            let close_res = conn.close().await;
            assert!(
                matches!(close_res, Ok(()) | Err(FrankenError::Abort)),
                "unexpected close result on cancelled cx: {close_res:?}"
            );
            assert!(count_dir_entries(temp_dir.path()).is_empty());
        }

        // 3. `close_in_place` releases an open spilled TEMP table's VFS temp file BEFORE returning!
        {
            let mut conn = Connection::open(db_path.to_str().unwrap())
                .await
                .expect("open close-cleanup conn");
            conn.execute("PRAGMA temp_store = FILE;").await.expect("file mode");
            conn.execute("PRAGMA temp.cache_size = -2;")
                .await
                .expect("2 KiB budget");
            conn.set_temp_store_directory(Some(temp_dir.path().to_path_buf()))
                .expect("set temp dir");
            conn.execute("CREATE TEMP TABLE live_spill(id INTEGER PRIMARY KEY, body TEXT);")
                .await
                .expect("create live_spill");
            conn.execute("INSERT INTO live_spill SELECT id, body FROM big;")
                .await
                .expect("populate live_spill");
            assert!(
                conn.temp_store_stats().active_temp_files >= 1,
                "live spilled TEMP table must hold an open temp VFS file before close"
            );
            assert!(
                !count_dir_entries(temp_dir.path()).is_empty(),
                "expected physical temp file on disk before close"
            );
            conn.close_in_place().await.expect("close_in_place");
            assert_eq!(
                conn.temp_store_stats().active_temp_files,
                0,
                "close_in_place must release every active temp file before returning"
            );
            assert!(
                count_dir_entries(temp_dir.path()).is_empty(),
                "close_in_place left residual files on disk: {:?}",
                count_dir_entries(temp_dir.path())
            );
        }
    });
}

#[test]
fn test_temp_store_nested_statements_triggers_cte_and_encrypted_db_zero_plaintext() {
    run_test(|| async {
        let dir = TempDir::new().expect("dir");
        let temp_dir = TempDir::new().expect("spill dir");

        // Part A: Nested statements (CTE + INSERT...SELECT + trigger writing into spilled TEMP table)
        // verified against stock SQLite.
        let conn = Connection::open(":memory:").await.expect("open fsqlite");
        let stock = rusqlite::Connection::open_in_memory().expect("open rusqlite");
        conn.set_temp_store_directory(Some(temp_dir.path().to_path_buf()))
            .expect("set temp dir");
        conn.execute("PRAGMA temp_store = FILE;").await.expect("file mode");
        conn.execute("PRAGMA temp.cache_size = -4;")
            .await
            .expect("4 KiB budget");

        let setup = [
            "CREATE TABLE src(id INTEGER PRIMARY KEY, grp TEXT, val TEXT);",
            "CREATE TEMP TABLE trig_audit(id INTEGER PRIMARY KEY, grp TEXT, note TEXT);",
            "CREATE TRIGGER temp.tr_src_ins AFTER INSERT ON src BEGIN INSERT INTO trig_audit VALUES (NEW.id, NEW.grp, 'audit-' || NEW.val); END;",
        ];
        for sql in setup {
            conn.execute(sql).await.expect("fsqlite setup");
            stock.execute(sql, []).expect("rusqlite setup");
        }
        for i in 1..=90_i64 {
            let sql = format!(
                "INSERT INTO src VALUES ({i}, 'g{}', 'nested-{i:03}-{}');",
                i % 5,
                "N".repeat(90)
            );
            conn.execute(&sql).await.expect("fsqlite ins");
            stock.execute(&sql, []).expect("rusqlite ins");
        }
        let cte_sql = "WITH ranked AS (SELECT grp, COUNT(*) AS cnt, MAX(note) AS hi FROM trig_audit GROUP BY grp) SELECT grp, cnt, length(hi) FROM ranked ORDER BY cnt DESC, grp ASC;";
        assert_eq!(
            fsqlite_query(&conn, cte_sql).await,
            rusqlite_query(&stock, cte_sql),
            "nested trigger + CTE + GROUP BY + ORDER BY must match stock SQLite"
        );
        let stats = conn.temp_store_stats();
        assert!(stats.temp_table_spill_events >= 1);
        conn.close().await.expect("close nested conn");
        assert!(count_dir_entries(temp_dir.path()).is_empty());

        // Part B: Encrypted database (`PRAGMA key`) with `PRAGMA temp_store = FILE` must encrypt
        // every temp spill block so ZERO plaintext sentinel bytes ever touch disk!
        let enc_db_path = dir.path().join("encrypted_spill.db");
        let enc_temp_dir = TempDir::new().expect("enc spill dir");
        let enc_conn = Connection::open(enc_db_path.to_str().unwrap())
            .await
            .expect("open enc db");
        enc_conn
            .execute("PRAGMA fsqlite.cipher_argon2_m_cost = 4096;")
            .await
            .expect("fast test m_cost");
        enc_conn
            .execute("PRAGMA fsqlite.cipher_argon2_t_cost = 1;")
            .await
            .expect("fast test t_cost");
        enc_conn
            .execute("PRAGMA fsqlite.cipher_argon2_p_cost = 1;")
            .await
            .expect("fast test p_cost");
        enc_conn
            .execute("PRAGMA key = 'top-secret-temp-spill-key';")
            .await
            .expect("set key");
        enc_conn
            .execute("PRAGMA temp_store = FILE;")
            .await
            .expect("temp_store FILE");
        enc_conn
            .execute("PRAGMA temp.cache_size = -2;")
            .await
            .expect("2 KiB budget");
        enc_conn
            .set_temp_store_directory(Some(enc_temp_dir.path().to_path_buf()))
            .expect("set enc temp dir");

        enc_conn
            .execute("CREATE TABLE secrets(id INTEGER PRIMARY KEY, secret TEXT);")
            .await
            .expect("create secrets");
        let sentinel = "SUPER_SECRET_TEMP_SPILL_MARKER_987654321";
        enc_conn.execute("BEGIN;").await.expect("begin");
        for i in 1..=80_i64 {
            let sql = format!(
                "INSERT INTO secrets VALUES ({i}, '{sentinel}-{i:03}-{}');",
                "S".repeat(80)
            );
            enc_conn.execute(&sql).await.expect("insert secret");
        }
        enc_conn.execute("COMMIT;").await.expect("commit");

        // Create a TEMP table & spill it while inspecting the actual temp file bytes on disk!
        enc_conn
            .execute("CREATE TEMP TABLE tmp_secrets(id INTEGER PRIMARY KEY, secret TEXT);")
            .await
            .expect("create tmp_secrets");
        enc_conn
            .execute("INSERT INTO tmp_secrets SELECT id, secret FROM secrets;")
            .await
            .expect("populate tmp_secrets");

        let enc_entries = count_dir_entries(enc_temp_dir.path());
        assert!(
            !enc_entries.is_empty(),
            "encrypted DB with PRAGMA temp_store=FILE must still spill to VFS temp files"
        );
        for path in &enc_entries {
            let raw = fs::read(path).expect("read encrypted temp file");
            assert!(!raw.is_empty(), "encrypted temp file should contain ciphertext");
            assert!(
                !raw.windows(sentinel.len()).any(|w| w == sentinel.as_bytes()),
                "plaintext sentinel leaked into temp spill file {}!",
                path.display()
            );
        }

        // Verify sorting over encrypted data returns exact decrypted rows and cleans up.
        let sorted = fsqlite_query(
            &enc_conn,
            "SELECT id, secret FROM tmp_secrets ORDER BY secret DESC LIMIT 3;",
        )
        .await;
        assert_eq!(sorted.len(), 3);
        assert_eq!(sorted[0][0], SqliteValue::Integer(80));
        enc_conn.close().await.expect("close enc conn");
        assert!(
            count_dir_entries(enc_temp_dir.path()).is_empty(),
            "encrypted temp spill files must be removed on close"
        );
    });
}

#[test]
fn test_temp_store_negative_control_stored_only_policy_fails() {
    run_test(|| async {
        // Acceptance predicate: under `PRAGMA temp_store = FILE` with a tiny 2 KiB budget
        // and a >16 KiB sort, `effective_mode_is_file` MUST be true AND `sorter_spill_runs >= 1`.
        let check_live_file_spill_predicate = |stats: fsqlite_vdbe::temp_store::TempStoreStatsSnapshot| -> bool {
            stats.configured_mode == 1
                && stats.effective_mode_is_file
                && stats.sorter_spill_runs >= 1
                && stats.temp_files_opened >= 1
                && stats.active_temp_files == 0
        };

        // 1. Planted negative control: stored-only / file-incapable policy reports
        //    `configured_mode == 1` (readback says FILE!) yet cannot spill to files.
        let inert_conn = Connection::open(":memory:").await.expect("open inert");
        inert_conn
            .execute("PRAGMA temp_store = FILE;")
            .await
            .expect("set FILE");
        inert_conn
            .execute("PRAGMA temp.cache_size = -2;")
            .await
            .expect("2 KiB budget");
        inert_conn.set_temp_file_backend(Some(Arc::new(StoredOnlyInertBackend)));
        inert_conn
            .execute("CREATE TABLE neg(id INTEGER PRIMARY KEY, body TEXT);")
            .await
            .expect("create neg");
        for i in 1..=80_i64 {
            let sql = format!("INSERT INTO neg VALUES ({i}, 'neg-{i:04}-{}');", "M".repeat(96));
            inert_conn.execute(&sql).await.expect("insert neg");
        }
        let _ = fsqlite_query(&inert_conn, "SELECT id, body FROM neg ORDER BY body DESC;").await;
        let inert_stats = inert_conn.temp_store_stats();
        assert_eq!(inert_stats.configured_mode, 1);
        assert!(
            !check_live_file_spill_predicate(inert_stats),
            "NEGATIVE CONTROL FAILED: stored-only inert policy must NOT satisfy live file-spill predicate: {inert_stats:?}"
        );
        inert_conn.close().await.expect("close inert");

        // Also verify TempStoreRuntime resolution unit-level negative control directly.
        let dummy_stats = Arc::new(TempStoreStats::new());
        let inert_rt = TempStoreRuntime::new(
            TempStoreMode::File,
            2048,
            None,
            Some(Arc::new(StoredOnlyInertBackend)),
            Arc::clone(&dummy_stats),
        );
        assert_eq!(inert_rt.effective_mode(), EffectiveTempStore::Memory);
        assert!(!inert_rt.is_file_backed());

        // 2. Live wired connection with real VFS backend MUST satisfy the exact same predicate.
        let temp_dir = TempDir::new().expect("live temp dir");
        let live_conn = Connection::open(":memory:").await.expect("open live");
        live_conn
            .set_temp_store_directory(Some(temp_dir.path().to_path_buf()))
            .expect("set dir");
        live_conn
            .execute("PRAGMA temp_store = FILE;")
            .await
            .expect("set FILE");
        live_conn
            .execute("PRAGMA temp.cache_size = -2;")
            .await
            .expect("2 KiB budget");
        live_conn
            .execute("CREATE TABLE pos(id INTEGER PRIMARY KEY, body TEXT);")
            .await
            .expect("create pos");
        for i in 1..=80_i64 {
            let sql = format!("INSERT INTO pos VALUES ({i}, 'pos-{i:04}-{}');", "M".repeat(96));
            live_conn.execute(&sql).await.expect("insert pos");
        }
        let _ = fsqlite_query(&live_conn, "SELECT id, body FROM pos ORDER BY body DESC;").await;
        let live_stats = live_conn.temp_store_stats();
        assert!(
            check_live_file_spill_predicate(live_stats),
            "live wired PRAGMA temp_store=FILE must satisfy live file-spill predicate: {live_stats:?}"
        );
        assert!(count_dir_entries(temp_dir.path()).is_empty());
        live_conn.close().await.expect("close live");
    });
}
