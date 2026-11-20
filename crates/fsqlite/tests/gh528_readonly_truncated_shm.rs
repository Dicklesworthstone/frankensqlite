//! GH#528 through the compat read-only open beads_rust (`br`) reads with:
//! a stock first attacher that dies after truncating `-shm` to 3 bytes left
//! every later read-only open answering `BusyRecovery`, with nothing holding
//! the database. Both WAL shapes from the field are covered: committed frames,
//! and the 32-byte header-only WAL of the reported tracker.
//!
//! The rebuild and its fail-closed boundaries are covered in
//! `fsqlite-core/tests/gh528_truncated_shm_first_attacher.rs`.
//!
//! bd-j2dtf: `read_only_opens_match_a_live_stock_process` compares the same
//! read-only opens with a stock SQLite child process, each on its own copy of
//! one file family.

#![cfg(all(unix, feature = "native"))]
#![recursion_limit = "512"]

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::Command;

use fsqlite::compat::{OpenFlags, open_with_flags};
use fsqlite_types::SqliteValue;
use fsqlite_wal::wal_index::parse_shm_header;

const ROWS: i64 = 30;
/// bd-j2dtf fixture: rows 1..=10 checkpointed into the main file, the rest
/// only in WAL frames.
const MAIN_FILE_ROWS: i64 = 10;
const STOCK_CHILD_TEST: &str = "bd_j2dtf_stock_child";
const STOCK_DB_ENV: &str = "FSQLITE_BD_J2DTF_STOCK_DB";
const STOCK_QUERY_ENV: &str = "FSQLITE_BD_J2DTF_STOCK_QUERY";
const STOCK_WITNESS: &str = "bd-j2dtf:";
/// One WAL-index region, and where its index entries start.
const SHM_REGION: usize = 32 * 1024;
const SHM_ENTRIES_OFFSET: usize = 136;

fn sidecar(path: &std::path::Path, suffix: &str) -> std::path::PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    name.into()
}

fn expected_rows() -> Vec<(i64, String)> {
    (1..=ROWS).map(|id| (id, format!("issue-{id}"))).collect()
}

async fn frank_rows(conn: &fsqlite::Connection, context: &str) -> Vec<(i64, String)> {
    conn.query("SELECT id, v FROM t ORDER BY id")
        .await
        .unwrap_or_else(|error| panic!("{context}: read-only query failed: {error}"))
        .iter()
        .map(|row| match row.values() {
            [SqliteValue::Integer(id), SqliteValue::Text(v)] => (*id, v.to_string()),
            other => panic!("{context}: unexpected row {other:?}"),
        })
        .collect()
}

#[test]
fn compat_read_only_open_rebuilds_a_three_byte_shm() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        for shape in ["frames", "header-only"] {
            let path = dir.path().join(format!("{shape}.db"));
            let writer = fsqlite_core::connection::Connection::open(path.to_str().unwrap())
                .await
                .expect("open writer");
            for sql in [
                "PRAGMA journal_mode=WAL",
                "PRAGMA wal_autocheckpoint=0",
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL)",
            ] {
                writer.execute(sql).await.expect(sql);
            }
            for (id, v) in expected_rows() {
                writer
                    .execute(&format!("INSERT INTO t VALUES ({id}, '{v}')"))
                    .await
                    .expect("insert");
            }
            if shape == "header-only" {
                writer
                    .execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    .await
                    .expect("settle the WAL");
            }
            writer
                .close_without_checkpoint()
                .await
                .expect("close writer");
            let wal = sidecar(&path, "-wal");
            if shape == "header-only" {
                let header = fsqlite_wal::WalHeader {
                    magic: fsqlite_wal::WAL_MAGIC_LE,
                    format_version: fsqlite_wal::WAL_FORMAT_VERSION,
                    page_size: 4096,
                    checkpoint_seq: 1,
                    salts: fsqlite_wal::WalSalts {
                        salt1: 0x0528_0528,
                        salt2: 0x1234_abcd,
                    },
                    checksum: fsqlite_wal::SqliteWalChecksum { s1: 0, s2: 0 },
                };
                std::fs::write(&wal, header.to_bytes().unwrap()).unwrap();
            }
            let wal_bytes = std::fs::read(&wal).unwrap();
            let shm = sidecar(&path, "-shm");
            std::fs::OpenOptions::new()
                .write(true)
                .open(&shm)
                .unwrap()
                .set_len(3)
                .unwrap();

            for attempt in 0..3 {
                let context = format!("{shape} WAL, open {attempt}");
                let reader =
                    open_with_flags(path.to_str().unwrap(), OpenFlags::SQLITE_OPEN_READ_ONLY)
                        .await
                        .unwrap_or_else(|error| {
                            panic!("{context}: read-only open failed: {error}")
                        });
                assert_eq!(
                    frank_rows(&reader, &context).await,
                    expected_rows(),
                    "{context}"
                );
                reader.close().await.expect("close reader");
            }
            assert_eq!(
                std::fs::read(&wal).unwrap(),
                wal_bytes,
                "{shape}: WAL unchanged"
            );
            let rebuilt = std::fs::read(&shm).unwrap();
            assert_eq!(rebuilt.len(), 32 * 1024, "{shape}: one full index region");
            assert!(
                fsqlite_wal::wal_index::parse_shm_header(&rebuilt)
                    .unwrap()
                    .is_some(),
                "{shape}: the rebuilt index has a valid header"
            );

            let stock = rusqlite::Connection::open_with_flags(
                &path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )
            .expect("stock read-only open");
            let count: i64 = stock
                .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
                .expect("stock count");
            assert_eq!(count, ROWS, "{shape}: stock agrees");
        }
    });
}

type Rows = Vec<(i64, String)>;

/// Each file of `db`'s family, by suffix ("" is the main file).
fn family(db: &Path) -> BTreeMap<String, Vec<u8>> {
    let name = db.file_name().unwrap().to_str().unwrap().to_owned();
    std::fs::read_dir(db.parent().unwrap())
        .unwrap()
        .filter_map(|entry| {
            let path = entry.unwrap().path();
            let suffix = path.file_name()?.to_str()?.strip_prefix(&name)?.to_owned();
            (suffix.is_empty() || suffix.starts_with('-'))
                .then(|| (suffix, std::fs::read(&path).unwrap()))
        })
        .collect()
}

/// A stock-written WAL database: rows 1..=10 checkpointed into the main
/// file, 11..=30 only in WAL frames, beside the `-shm` stock published. The
/// returned writer stays attached: closing the last stock connection would
/// checkpoint the WAL away, so copies are taken while it is open.
fn stock_fixture(dir: &Path) -> (rusqlite::Connection, PathBuf) {
    let (writer, db, _) = stock_fixture_with_stale_shm(dir);
    (writer, db)
}

/// [`stock_fixture`], plus the writer's `-shm` as it stood 10 commits before
/// the end: a valid index that misses the WAL's last 10 frames.
fn stock_fixture_with_stale_shm(dir: &Path) -> (rusqlite::Connection, PathBuf, Vec<u8>) {
    let db = dir.join("source.db");
    let writer = rusqlite::Connection::open(&db).unwrap();
    writer
        .execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
             CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL);",
        )
        .unwrap();
    let mut stale_shm = Vec::new();
    for (id, v) in expected_rows() {
        writer
            .execute("INSERT INTO t VALUES (?1, ?2)", rusqlite::params![id, v])
            .unwrap();
        if id == MAIN_FILE_ROWS {
            writer
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
                .unwrap();
        }
        if id == ROWS - 10 {
            stale_shm = std::fs::read(sidecar(&db, "-shm")).unwrap();
        }
    }
    (writer, db, stale_shm)
}

fn copy_family(source: &Path, to: &Path) -> PathBuf {
    std::fs::create_dir_all(to).unwrap();
    let db = to.join("copy.db");
    for suffix in ["", "-wal", "-shm"] {
        std::fs::copy(sidecar(source, suffix), sidecar(&db, suffix)).unwrap();
    }
    db
}

/// bd-tv006: set, the stock child opens read-write and, after its read,
/// tries [`insert_next_row`]'s row.
const STOCK_WRITE_ENV: &str = "FSQLITE_BD_TV006_STOCK_WRITE";

/// The row a read-write arm tries to add after its read.
fn insert_next_row() -> (i64, String) {
    (ROWS + 1, format!("issue-{}", ROWS + 1))
}

/// Child: a stock SQLite read through `file:<db>?<query>`, as witness lines;
/// with [`STOCK_WRITE_ENV`], a read-write open that then tries an INSERT.
#[test]
fn bd_j2dtf_stock_child() {
    let (Some(db), Some(query)) = (
        std::env::var_os(STOCK_DB_ENV),
        std::env::var_os(STOCK_QUERY_ENV),
    ) else {
        return;
    };
    let write = std::env::var_os(STOCK_WRITE_ENV).is_some();
    let uri = format!("file:{}?{}", db.to_str().unwrap(), query.to_str().unwrap());
    let access = if write {
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
    } else {
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
    };
    let read = || -> rusqlite::Result<(Rows, Option<rusqlite::Result<usize>>)> {
        let conn = rusqlite::Connection::open_with_flags(
            &uri,
            access | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        )?;
        let rows = conn
            .prepare("SELECT id, v FROM t ORDER BY id")?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<Rows>>()?;
        let insert = write.then(|| {
            let (id, v) = insert_next_row();
            conn.execute("INSERT INTO t VALUES (?1, ?2)", rusqlite::params![id, v])
        });
        Ok((rows, insert))
    };
    match read() {
        Ok((rows, insert)) => {
            for (id, v) in rows {
                println!("{STOCK_WITNESS}row {id} {v}");
            }
            match insert {
                Some(Ok(_)) => println!("{STOCK_WITNESS}write ok"),
                Some(Err(error)) => println!("{STOCK_WITNESS}write error {error}"),
                None => {}
            }
            println!("{STOCK_WITNESS}ok");
        }
        Err(error) => println!("{STOCK_WITNESS}error {error}"),
    }
}

/// The stock arm: a separate process, so its POSIX locks are its own.
fn stock_read(db: &Path, query: &str) -> Result<Rows, String> {
    let (rows, write) = stock_child(db, query, false)?;
    assert!(write.is_none(), "a read-only stock arm writes nothing");
    Ok(rows)
}

/// [`stock_read`] on a read-write connection that tries [`insert_next_row`]
/// after its read: the rows, and the INSERT's answer.
fn stock_read_then_write(db: &Path, query: &str) -> Result<(Rows, Result<(), String>), String> {
    let (rows, write) = stock_child(db, query, true)?;
    Ok((
        rows,
        write.expect("the read-write stock arm tried its INSERT"),
    ))
}

fn stock_child(
    db: &Path,
    query: &str,
    write: bool,
) -> Result<(Rows, Option<Result<(), String>>), String> {
    let mut child = Command::new(std::env::current_exe().unwrap());
    child
        .args([
            STOCK_CHILD_TEST,
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(STOCK_DB_ENV, db)
        .env(STOCK_QUERY_ENV, query);
    if write {
        child.env(STOCK_WRITE_ENV, "1");
    }
    let output = child.output().expect("run the stock child");
    assert!(output.status.success(), "stock child failed: {output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    let mut rows = Vec::new();
    let mut insert = None;
    for line in stdout.lines() {
        // The harness's `test ... ` banner may precede the first witness.
        let Some((_, witness)) = line.split_once(STOCK_WITNESS) else {
            continue;
        };
        if let Some(row) = witness.strip_prefix("row ") {
            let (id, v) = row.split_once(' ').unwrap();
            rows.push((id.parse().unwrap(), v.to_owned()));
        } else if witness == "write ok" {
            insert = Some(Ok(()));
        } else if let Some(error) = witness.strip_prefix("write error ") {
            insert = Some(Err(error.to_owned()));
        } else if witness == "ok" {
            return Ok((rows, insert));
        } else if let Some(error) = witness.strip_prefix("error ") {
            return Err(error.to_owned());
        }
    }
    panic!("the stock child printed no result: {stdout}");
}

async fn frank_read(db: &Path, query: &str) -> Result<Rows, String> {
    let uri = format!("file:{}?{query}", db.to_str().unwrap());
    let conn = open_with_flags(
        &uri,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .await
    .map_err(|error| error.to_string())?;
    let rows = conn
        .query("SELECT id, v FROM t ORDER BY id")
        .await
        .map_err(|error| error.to_string())
        .map(|rows| {
            rows.iter()
                .map(|row| match row.values() {
                    [SqliteValue::Integer(id), SqliteValue::Text(v)] => (*id, v.to_string()),
                    other => panic!("unexpected row {other:?}"),
                })
                .collect()
        });
    conn.close().await.map_err(|error| error.to_string())?;
    rows
}

/// [`frank_read`] on a read-write compat open that tries [`insert_next_row`]
/// after its read, as [`stock_read_then_write`].
async fn frank_read_then_write(
    db: &Path,
    query: &str,
) -> Result<(Rows, Result<(), String>), String> {
    let uri = format!("file:{}?{query}", db.to_str().unwrap());
    let conn = open_with_flags(
        &uri,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_URI,
    )
    .await
    .map_err(|error| error.to_string())?;
    let rows = frank_rows(&conn, "read-write arm").await;
    let (id, v) = insert_next_row();
    let insert = conn
        .execute(&format!("INSERT INTO t VALUES ({id}, '{v}')"))
        .await
        .map(|_| ())
        .map_err(|error| error.to_string());
    conn.close().await.map_err(|error| error.to_string())?;
    Ok((rows, insert))
}

/// The parts of a rebuilt `-shm` that the WAL determines: both header
/// copies' fields except the change counter and the checksum over it, and
/// every index entry. Reader marks and backfill bookkeeping are left out.
fn rebuilt_index(shm: &[u8], context: &str) -> (fsqlite_wal::wal_index::WalIndexHdr, Vec<u8>) {
    assert_eq!(shm.len(), SHM_REGION, "{context}: one full index region");
    let (mut header, _) = parse_shm_header(shm)
        .unwrap()
        .unwrap_or_else(|| panic!("{context}: the rebuilt -shm has no valid header"));
    header.i_change = 0;
    header.a_cksum = [0, 0];
    (header, shm[SHM_ENTRIES_OFFSET..].to_vec())
}

/// Whether this process can open `path` for writing. A 0444 mode does not
/// bind a process that bypasses file modes (root, as on the vmi workers).
fn writable(path: &Path) -> bool {
    std::fs::OpenOptions::new().write(true).open(path).is_ok()
}

/// `chattr +i` on files, which no process may open for writing, root
/// included; cleared again on drop so the temporary directory can be removed.
struct ImmutableFiles(Vec<PathBuf>);

impl ImmutableFiles {
    fn set(paths: &[PathBuf]) -> Option<Self> {
        let mut guard = Self(Vec::new());
        for path in paths {
            let set = Command::new("chattr")
                .arg("+i")
                .arg(path)
                .status()
                .is_ok_and(|status| status.success());
            if !set {
                return None;
            }
            guard.0.push(path.clone());
        }
        Some(guard)
    }
}

impl Drop for ImmutableFiles {
    fn drop(&mut self) {
        for path in &self.0 {
            let _ = Command::new("chattr").arg("-i").arg(path).status();
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReadOnlyShape {
    /// Stock's first-attacher marker left by one that died (GH#528).
    ShmThreeBytes,
    /// A full region nobody initialized.
    ShmZeroFilled,
    /// A valid index the reader may not write (GH#535's comment).
    ShmValidReadOnly,
    /// The 3-byte marker, unwritable.
    ShmThreeBytesReadOnly,
    /// The uninitialized region, unwritable.
    ShmZeroFilledReadOnly,
    /// A valid index from 10 commits ago, unwritable: trusting it would miss
    /// the WAL's last 10 frames.
    ShmStaleReadOnly,
    /// `immutable=1`: no locks, no `-wal`, no `-shm`.
    Immutable,
}

impl ReadOnlyShape {
    /// The `-shm` neither engine may write.
    fn shm_unwritable(self) -> bool {
        matches!(
            self,
            Self::ShmValidReadOnly
                | Self::ShmThreeBytesReadOnly
                | Self::ShmZeroFilledReadOnly
                | Self::ShmStaleReadOnly
        )
    }
}

/// bd-j2dtf: a read-only open beside a live stock process's answer for the
/// same file family. Stock (here the bundled 3.54.1 in a child process; the
/// bead also measured 3.46.1) rebuilds a short or uninitialized writable
/// `-shm`, reads every row and leaves the main file and the WAL unchanged.
/// FrankenSQLite must give the same rows and the same rebuilt index.
/// `immutable=1` (bd-j2dtf item 3) must read what stock reads, the main file
/// alone, and write nothing.
///
/// bd-j2dtf item 2: over a `-shm` neither engine may write (0444, or chattr
/// +i where the process is root), valid, 3-byte, zero-filled or stale, with
/// no live connection on the copy, stock reads the WAL through a private
/// heap index (`walBeginShmUnreliable`) and writes nothing. FrankenSQLite
/// must read the same 30 rows and write nothing. The stale shape is the
/// negative a naive "map it read-only and trust it" fails: its index misses
/// the last 10 frames, so that reader returns 20 rows.
#[test]
fn read_only_opens_match_a_live_stock_process() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let (writer, source, stale_shm) = stock_fixture_with_stale_shm(dir.path());
        for shape in [
            ReadOnlyShape::ShmThreeBytes,
            ReadOnlyShape::ShmZeroFilled,
            ReadOnlyShape::ShmValidReadOnly,
            ReadOnlyShape::ShmThreeBytesReadOnly,
            ReadOnlyShape::ShmZeroFilledReadOnly,
            ReadOnlyShape::ShmStaleReadOnly,
            ReadOnlyShape::Immutable,
        ] {
            let context = format!("{shape:?}");
            let arms = [
                copy_family(&source, &dir.path().join(format!("{shape:?}-stock"))),
                copy_family(&source, &dir.path().join(format!("{shape:?}-frank"))),
            ];
            for db in &arms {
                let shm = sidecar(db, "-shm");
                match shape {
                    ReadOnlyShape::ShmThreeBytes | ReadOnlyShape::ShmThreeBytesReadOnly => {
                        std::fs::OpenOptions::new()
                            .write(true)
                            .open(&shm)
                            .unwrap()
                            .set_len(3)
                            .unwrap();
                    }
                    ReadOnlyShape::ShmZeroFilled | ReadOnlyShape::ShmZeroFilledReadOnly => {
                        std::fs::write(&shm, vec![0_u8; SHM_REGION]).unwrap();
                    }
                    ReadOnlyShape::ShmStaleReadOnly => {
                        std::fs::write(&shm, &stale_shm).unwrap();
                    }
                    ReadOnlyShape::ShmValidReadOnly | ReadOnlyShape::Immutable => {}
                }
                if shape.shm_unwritable() {
                    std::fs::set_permissions(&shm, std::fs::Permissions::from_mode(0o444)).unwrap();
                }
            }
            // The 0444 shape needs a `-shm` neither engine can write. Root
            // ignores the mode, so there the files are made immutable; where
            // that is impossible the shape cannot be built and is skipped,
            // loudly, instead of comparing two engines that may both write.
            let shms = arms
                .iter()
                .map(|db| sidecar(db, "-shm"))
                .collect::<Vec<_>>();
            let _immutable = if shape.shm_unwritable() && shms.iter().any(|shm| writable(shm)) {
                match ImmutableFiles::set(&shms) {
                    Some(guard) if !shms.iter().any(|shm| writable(shm)) => {
                        eprintln!(
                            "event=bd_j2dtf_read_only_open shape={shape:?} note=\"0444 does not \
                             bind this process (root); the -shm copies are chattr +i instead\""
                        );
                        Some(guard)
                    }
                    _ => {
                        eprintln!(
                            "event=bd_j2dtf_read_only_open shape={shape:?} SKIPPED: this process \
                             writes a 0444 -shm (root) and chattr +i is unavailable here, so an \
                             unwritable -shm cannot be built"
                        );
                        continue;
                    }
                }
            } else {
                None
            };
            let query = if shape == ReadOnlyShape::Immutable {
                "mode=ro&immutable=1"
            } else {
                "mode=ro"
            };
            let [stock_db, frank_db] = &arms;
            let before = family(stock_db);
            assert_eq!(family(frank_db), before, "{context}: identical copies");

            let stock = stock_read(stock_db, query);
            let stock_after = family(stock_db);
            let frank = frank_read(frank_db, query).await;
            let frank_after = family(frank_db);
            eprintln!(
                "event=bd_j2dtf_read_only_open shape={shape:?} stock={:?} frank={:?}",
                stock.as_ref().map(Vec::len),
                frank.as_ref().map(Vec::len)
            );

            // The oracle's own answer, so a changed stock cannot pass silently.
            let stock_rows = match shape {
                ReadOnlyShape::Immutable => {
                    let rows: Rows = expected_rows()
                        .into_iter()
                        .take(usize::try_from(MAIN_FILE_ROWS).unwrap())
                        .collect();
                    assert_eq!(
                        stock,
                        Ok(rows.clone()),
                        "{context}: stock reads the main file only"
                    );
                    rows
                }
                _ => {
                    assert_eq!(
                        stock,
                        Ok(expected_rows()),
                        "{context}: stock reads every row"
                    );
                    expected_rows()
                }
            };
            for (suffix, bytes) in &before {
                if suffix != "-shm" {
                    assert_eq!(
                        stock_after.get(suffix),
                        Some(bytes),
                        "{context}: stock {suffix}"
                    );
                    assert_eq!(
                        frank_after.get(suffix),
                        Some(bytes),
                        "{context}: fsqlite must leave {suffix:?} unchanged"
                    );
                }
            }
            assert_eq!(
                frank_after.keys().collect::<Vec<_>>(),
                stock_after.keys().collect::<Vec<_>>(),
                "{context}: the same files exist after the open"
            );

            match shape {
                ReadOnlyShape::ShmThreeBytes | ReadOnlyShape::ShmZeroFilled => {
                    assert_eq!(
                        frank,
                        Ok(stock_rows),
                        "{context}: fsqlite reads what stock reads"
                    );
                    assert_eq!(
                        rebuilt_index(&frank_after["-shm"], &format!("{context} fsqlite")),
                        rebuilt_index(&stock_after["-shm"], &format!("{context} stock")),
                        "{context}: fsqlite rebuilds the index stock rebuilds"
                    );
                }
                ReadOnlyShape::ShmValidReadOnly
                | ReadOnlyShape::ShmThreeBytesReadOnly
                | ReadOnlyShape::ShmZeroFilledReadOnly
                | ReadOnlyShape::ShmStaleReadOnly => {
                    assert_eq!(stock_after, before, "{context}: stock writes nothing");
                    assert_eq!(frank_after, before, "{context}: fsqlite must write nothing");
                    assert_eq!(
                        frank,
                        Ok(stock_rows),
                        "{context}: fsqlite reads what stock reads through a private index"
                    );
                }
                ReadOnlyShape::Immutable => {
                    assert_eq!(stock_after, before, "{context}: stock writes nothing");
                    assert_eq!(frank_after, before, "{context}: fsqlite must write nothing");
                    // A plain mode=ro read would also see the 20 WAL-only
                    // rows; stock ignores the WAL of an immutable file.
                    assert_eq!(
                        frank,
                        Ok(stock_rows),
                        "{context}: fsqlite reads the main file alone, as stock does"
                    );
                }
            }
        }
        drop(writer);
    });
}

/// bd-j2dtf item 3: `immutable=1` takes no locks and creates no file, as
/// stock opens an immutable file (like a temp file: no locking, no WAL).
///
/// - A WAL family copied without its `-shm`: both engines read the main
///   file's rows, and neither creates a `-shm`, a `-wal` or anything else.
/// - A rollback-journal database whose EXCLUSIVE lock this process holds
///   (an open `BEGIN EXCLUSIVE` with a pending row): stock in a child reads
///   the committed rows immutably. Negative: a plain `mode=ro` read, which
///   locks, is refused by both engines, so the lock binds fsqlite and an
///   implementation that locks would fail the immutable read.
#[test]
fn immutable_open_takes_no_locks_and_creates_no_sidecar() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let (writer, source) = stock_fixture(dir.path());
        let main_rows: Rows = expected_rows()
            .into_iter()
            .take(usize::try_from(MAIN_FILE_ROWS).unwrap())
            .collect();
        let mut arms = Vec::new();
        for engine in ["stock", "frank"] {
            let to = dir.path().join(format!("no-shm-{engine}"));
            std::fs::create_dir_all(&to).unwrap();
            let db = to.join("copy.db");
            for suffix in ["", "-wal"] {
                std::fs::copy(sidecar(&source, suffix), sidecar(&db, suffix)).unwrap();
            }
            arms.push(db);
        }
        let before = family(&arms[0]);
        assert_eq!(family(&arms[1]), before, "identical copies");
        assert!(!before.contains_key("-shm"), "the copies have no -shm");
        let stock = stock_read(&arms[0], "immutable=1");
        let frank = frank_read(&arms[1], "immutable=1").await;
        eprintln!(
            "event=bd_j2dtf_immutable shape=no_shm stock={:?} frank={:?}",
            stock.as_ref().map(Vec::len),
            frank.as_ref().map(Vec::len)
        );
        assert_eq!(
            stock,
            Ok(main_rows.clone()),
            "stock reads the main file alone"
        );
        assert_eq!(
            frank,
            Ok(main_rows.clone()),
            "fsqlite reads what stock reads"
        );
        assert_eq!(
            family(&arms[0]),
            before,
            "stock creates and changes nothing"
        );
        assert_eq!(
            family(&arms[1]),
            before,
            "fsqlite creates and changes nothing"
        );
        drop(writer);

        let locked = dir.path().join("locked").join("rollback.db");
        std::fs::create_dir_all(locked.parent().unwrap()).unwrap();
        let holder = rusqlite::Connection::open(&locked).unwrap();
        holder
            .execute_batch(
                "PRAGMA journal_mode=DELETE;
                 CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL);",
            )
            .unwrap();
        for (id, v) in &main_rows {
            holder
                .execute("INSERT INTO t VALUES (?1, ?2)", rusqlite::params![id, v])
                .unwrap();
        }
        holder
            .execute_batch("BEGIN EXCLUSIVE; INSERT INTO t VALUES (99, 'uncommitted');")
            .unwrap();
        let stock_plain = stock_read(&locked, "mode=ro");
        let frank_plain = frank_read(&locked, "mode=ro").await;
        let stock_immutable = stock_read(&locked, "immutable=1");
        let frank_immutable = frank_read(&locked, "immutable=1").await;
        eprintln!(
            "event=bd_j2dtf_immutable shape=exclusive_lock stock_plain={stock_plain:?} \
             frank_plain={:?} stock_immutable={:?} frank_immutable={:?}",
            frank_plain.as_ref().map(Vec::len),
            stock_immutable.as_ref().map(Vec::len),
            frank_immutable.as_ref().map(Vec::len)
        );
        assert!(
            stock_plain.is_err(),
            "the stock oracle: EXCLUSIVE refuses a locking read"
        );
        assert!(
            frank_plain.is_err(),
            "negative control: the EXCLUSIVE lock refuses fsqlite's locking read too"
        );
        assert_eq!(
            stock_immutable,
            Ok(main_rows.clone()),
            "stock reads an immutable file without locking"
        );
        assert_eq!(
            frank_immutable,
            Ok(main_rows),
            "fsqlite reads an immutable file without locking, as stock does"
        );
        holder.execute_batch("ROLLBACK").unwrap();
    });
}

/// bd-j2dtf item 3, negative: under `immutable=1` a table that exists only in
/// the WAL does not exist. Stock reads an immutable file's main file alone and
/// answers "no such table: t", so fsqlite must too, and neither writes a file.
/// An implementation that reads the WAL would find the table. Control: stock's
/// plain `mode=ro` open of the same family does see it.
#[test]
fn immutable_open_does_not_see_a_table_that_exists_only_in_the_wal() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("source.db");
        let writer = rusqlite::Connection::open(&db).unwrap();
        writer
            .execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
                 CREATE TABLE m(x); INSERT INTO m VALUES (1);",
            )
            .unwrap();
        writer
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
            .unwrap();
        writer
            .execute_batch(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL);
                 INSERT INTO t VALUES (1, 'wal-only');",
            )
            .unwrap();
        let stock_arm = copy_family(&db, &dir.path().join("stock"));
        let frank_arm = copy_family(&db, &dir.path().join("frank"));
        let control_arm = copy_family(&db, &dir.path().join("control"));
        let (stock_before, frank_before) = (family(&stock_arm), family(&frank_arm));

        assert_eq!(
            stock_read(&control_arm, "mode=ro"),
            Ok(vec![(1, "wal-only".to_owned())]),
            "control: a plain read-only open sees the table the WAL created"
        );
        let stock = stock_read(&stock_arm, "immutable=1");
        let frank = frank_read(&frank_arm, "immutable=1").await;
        let stock_error = stock.expect_err("stock: immutable=1 ignores the WAL");
        assert!(
            stock_error.contains("no such table"),
            "stock: {stock_error}"
        );
        let frank_error =
            frank.expect_err("fsqlite must not see a table that exists only in the WAL");
        assert!(
            frank_error.contains("no such table"),
            "fsqlite must answer as stock does ({stock_error}), got: {frank_error}"
        );
        assert_eq!(family(&stock_arm), stock_before, "stock writes nothing");
        assert_eq!(family(&frank_arm), frank_before, "fsqlite writes nothing");
        drop(writer);
    });
}

const HOLDER_CHILD_TEST: &str = "bd_tv006_stock_holder_child";
const HOLDER_DB_ENV: &str = "FSQLITE_BD_TV006_HOLDER_DB";

/// Child: a stock SQLite connection that attaches to `db` (it reads, so it
/// maps the `-shm` and holds its dead-man-switch byte), says `<db>.held`, and
/// stays attached until `<db>.release` appears.
#[test]
fn bd_tv006_stock_holder_child() {
    let Some(db) = std::env::var_os(HOLDER_DB_ENV) else {
        return;
    };
    let db = PathBuf::from(db);
    let conn = rusqlite::Connection::open(&db).unwrap();
    let count: i64 = conn
        .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, ROWS, "the holder reads every row");
    std::fs::write(sidecar(&db, ".held"), b"").unwrap();
    let started = std::time::Instant::now();
    while !sidecar(&db, ".release").exists() {
        assert!(
            started.elapsed() < std::time::Duration::from_secs(120),
            "the holder was never released"
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    drop(conn);
}

/// A live stock peer in its own process, so its POSIX locks are its own.
struct StockHolder {
    child: Option<std::process::Child>,
    db: PathBuf,
}

impl StockHolder {
    fn attach(db: &Path) -> Self {
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                HOLDER_CHILD_TEST,
                "--exact",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(HOLDER_DB_ENV, db)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn the stock holder");
        let mut holder = Self {
            child: Some(child),
            db: db.to_path_buf(),
        };
        let started = std::time::Instant::now();
        while !sidecar(db, ".held").exists() {
            let exited = holder
                .child
                .as_mut()
                .expect("holder spawned")
                .try_wait()
                .expect("holder status");
            assert!(exited.is_none(), "the stock holder exited before attaching");
            assert!(
                started.elapsed() < std::time::Duration::from_secs(60),
                "the stock holder never attached"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        holder
    }
}

impl Drop for StockHolder {
    fn drop(&mut self) {
        let _ = std::fs::write(sidecar(&self.db, ".release"), b"");
        if let Some(mut child) = self.child.take() {
            let _ = child.wait();
        }
    }
}

/// The shapes of [`bd_tv006_first_attacher_rebuilds_like_stock`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FirstAttachShape {
    /// A valid index from 10 commits ago, writable, no live connection: stock's
    /// first attacher rebuilds it. Trusting it reads 20 of the 30 rows.
    StaleNoLivePeer,
    /// The same stale `-shm`, but a live stock connection attached first (it
    /// rebuilt the index as the first attacher): nothing is rewritten.
    StaleLivePeer,
    /// No `-shm`, in a writable directory: stock creates and builds it.
    Missing,
    /// Both header copies agree but their checksum is wrong: stock rebuilds.
    InvalidChecksum,
    /// `readonly_shm=1` over a 3-byte `-shm`: read without writing anything.
    ReadonlyShmShort,
    /// Negative: `readonly_shm=1` over the stale index still reads every row
    /// (a private index) and writes nothing.
    ReadonlyShmStale,
    /// Negative: a valid, current index with no live connection is used, not
    /// rewritten.
    ValidCurrentNoLivePeer,
}

/// bd-tv006: a read-only open that is stock's first attacher (no connection
/// attached, a `-shm` it may write or create) rebuilds the WAL index as stock
/// does instead of trusting it, compared with a live stock 3.54.1 process on
/// its own copy of each file family: the same rows, the main file and WAL
/// unchanged, and where stock rebuilt or created the `-shm`, the same rebuilt
/// index. `readonly_shm=1` reads every row and writes nothing. Red on main:
/// the stale index read 20 of 30 rows; a missing or invalid `-shm` answered
/// `BusyRecovery`; `readonly_shm=1` was ignored and a 3-byte `-shm` was
/// rewritten. The unwritable shapes stay in
/// [`read_only_opens_match_a_live_stock_process`].
#[test]
fn bd_tv006_first_attacher_rebuilds_like_stock() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let (writer, source, stale_shm) = stock_fixture_with_stale_shm(dir.path());
        for shape in [
            FirstAttachShape::StaleNoLivePeer,
            FirstAttachShape::StaleLivePeer,
            FirstAttachShape::Missing,
            FirstAttachShape::InvalidChecksum,
            FirstAttachShape::ReadonlyShmShort,
            FirstAttachShape::ReadonlyShmStale,
            FirstAttachShape::ValidCurrentNoLivePeer,
        ] {
            let context = format!("{shape:?}");
            let arms = [
                copy_family(&source, &dir.path().join(format!("tv006-{shape:?}-stock"))),
                copy_family(&source, &dir.path().join(format!("tv006-{shape:?}-frank"))),
            ];
            let mut peers = Vec::new();
            for db in &arms {
                let shm = sidecar(db, "-shm");
                match shape {
                    FirstAttachShape::StaleNoLivePeer
                    | FirstAttachShape::StaleLivePeer
                    | FirstAttachShape::ReadonlyShmStale => {
                        std::fs::write(&shm, &stale_shm).unwrap()
                    }
                    FirstAttachShape::Missing => std::fs::remove_file(&shm).unwrap(),
                    FirstAttachShape::InvalidChecksum => {
                        let mut bytes = std::fs::read(&shm).unwrap();
                        // Both 48-byte header copies keep agreeing; their
                        // checksum (aCksum, bytes 40..48 of each) is wrong.
                        bytes[40] ^= 0x5a;
                        bytes[48 + 40] ^= 0x5a;
                        std::fs::write(&shm, bytes).unwrap();
                    }
                    FirstAttachShape::ReadonlyShmShort => {
                        std::fs::OpenOptions::new()
                            .write(true)
                            .open(&shm)
                            .unwrap()
                            .set_len(3)
                            .unwrap();
                    }
                    FirstAttachShape::ValidCurrentNoLivePeer => {}
                }
                if shape == FirstAttachShape::StaleLivePeer {
                    peers.push(StockHolder::attach(db));
                }
            }
            let query = match shape {
                FirstAttachShape::ReadonlyShmShort | FirstAttachShape::ReadonlyShmStale => {
                    "mode=ro&readonly_shm=1"
                }
                _ => "mode=ro",
            };
            let [stock_db, frank_db] = &arms;
            let stock_before = family(stock_db);
            let frank_before = family(frank_db);
            assert_eq!(frank_before, stock_before, "{context}: identical copies");

            let stock = stock_read(stock_db, query);
            let stock_after = family(stock_db);
            let frank = frank_read(frank_db, query).await;
            let frank_after = family(frank_db);
            eprintln!(
                "event=bd_tv006_first_attach shape={shape:?} stock={:?} frank={:?}",
                stock.as_ref().map(Vec::len),
                frank.as_ref().map(Vec::len)
            );

            // The oracle's own answer, so a changed stock cannot pass silently.
            assert_eq!(
                stock,
                Ok(expected_rows()),
                "{context}: stock reads every row"
            );
            assert_eq!(
                frank,
                Ok(expected_rows()),
                "{context}: fsqlite reads what stock reads"
            );
            for suffix in ["", "-wal"] {
                assert_eq!(
                    stock_after.get(suffix),
                    stock_before.get(suffix),
                    "{context}: stock leaves {suffix:?} unchanged"
                );
                assert_eq!(
                    frank_after.get(suffix),
                    frank_before.get(suffix),
                    "{context}: fsqlite must leave {suffix:?} unchanged"
                );
            }
            assert_eq!(
                frank_after.keys().collect::<Vec<_>>(),
                stock_after.keys().collect::<Vec<_>>(),
                "{context}: the same files exist after the open"
            );
            match shape {
                FirstAttachShape::StaleNoLivePeer
                | FirstAttachShape::Missing
                | FirstAttachShape::InvalidChecksum => {
                    assert_ne!(
                        stock_after.get("-shm"),
                        stock_before.get("-shm"),
                        "{context}: stock rebuilds or creates the -shm"
                    );
                    assert_eq!(
                        rebuilt_index(&frank_after["-shm"], &format!("{context} fsqlite")),
                        rebuilt_index(&stock_after["-shm"], &format!("{context} stock")),
                        "{context}: fsqlite rebuilds the index stock rebuilds"
                    );
                }
                FirstAttachShape::StaleLivePeer | FirstAttachShape::ValidCurrentNoLivePeer => {
                    assert_eq!(
                        frank_after, frank_before,
                        "{context}: fsqlite must not rewrite a live or current index"
                    );
                }
                FirstAttachShape::ReadonlyShmShort | FirstAttachShape::ReadonlyShmStale => {
                    assert_eq!(stock_after, stock_before, "{context}: stock writes nothing");
                    assert_eq!(
                        frank_after, frank_before,
                        "{context}: readonly_shm=1 must write nothing"
                    );
                }
            }
            drop(peers);
        }
        drop(writer);
    });
}

/// A rollback-journal database holding rows 1..=ROWS, with no `-wal` and no
/// `-shm`.
fn rollback_fixture(dir: &Path) -> PathBuf {
    let db = dir.join("rollback.db");
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL);")
        .unwrap();
    for (id, v) in expected_rows() {
        conn.execute("INSERT INTO t VALUES (?1, ?2)", rusqlite::params![id, v])
            .unwrap();
    }
    drop(conn);
    assert!(
        !sidecar(&db, "-wal").exists() && !sidecar(&db, "-shm").exists(),
        "the rollback fixture has no WAL sidecars"
    );
    db
}

/// The shapes of [`bd_tv006_readonly_shm_refusals_and_writes_match_stock`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReadonlyShmShape {
    /// `mode=ro&readonly_shm=1` with no `-shm`: stock answers
    /// `SQLITE_CANTOPEN`.
    MissingShmReadOnly,
    /// A read-write `readonly_shm=1` connection over the stale index with no
    /// live connection: stock reads every row (through a private index), its
    /// INSERT answers `SQLITE_READONLY`, and nothing is written.
    StaleIndexReadWrite,
    /// A read-write `readonly_shm=1` connection with no `-shm`:
    /// `SQLITE_CANTOPEN`, and nothing is written.
    MissingShmReadWrite,
    /// A rollback-journal database: `readonly_shm=1` changes nothing, and the
    /// INSERT commits.
    RollbackJournalReadWrite,
}

/// The rows a read read, and its INSERT's answer when it tried one.
type ReadThenWrite = Result<(Rows, Option<Result<(), String>>), String>;

/// bd-tv006: `readonly_shm=1` where stock refuses or writes, beside a live
/// stock 3.54.1 process on its own copy of each family. Stock applies the
/// parameter to read-write connections too: over a WAL database it reads,
/// answers its INSERT with `SQLITE_READONLY` and writes nothing; with no
/// `-shm` it answers `SQLITE_CANTOPEN`; over a rollback-journal database it reads and
/// writes as without it. Red before the fix: the read-write pairing was
/// refused as not implemented, and a read-only open with no `-shm` answered
/// `BusyRecovery`.
#[test]
fn bd_tv006_readonly_shm_refusals_and_writes_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let (writer, source, stale_shm) = stock_fixture_with_stale_shm(dir.path());
        let rollback = rollback_fixture(dir.path());
        let rows_in = |db: &Path| -> i64 {
            rusqlite::Connection::open(db)
                .unwrap()
                .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
                .unwrap()
        };
        for shape in [
            ReadonlyShmShape::MissingShmReadOnly,
            ReadonlyShmShape::StaleIndexReadWrite,
            ReadonlyShmShape::MissingShmReadWrite,
            ReadonlyShmShape::RollbackJournalReadWrite,
        ] {
            let context = format!("{shape:?}");
            let copy = |arm: &str| -> PathBuf {
                let to = dir.path().join(format!("tv006-rs-{shape:?}-{arm}"));
                if shape == ReadonlyShmShape::RollbackJournalReadWrite {
                    std::fs::create_dir_all(&to).unwrap();
                    let db = to.join("copy.db");
                    std::fs::copy(&rollback, &db).unwrap();
                    db
                } else {
                    copy_family(&source, &to)
                }
            };
            let [stock_db, frank_db] = [copy("stock"), copy("frank")];
            for db in [&stock_db, &frank_db] {
                let shm = sidecar(db, "-shm");
                match shape {
                    ReadonlyShmShape::MissingShmReadOnly
                    | ReadonlyShmShape::MissingShmReadWrite => std::fs::remove_file(&shm).unwrap(),
                    ReadonlyShmShape::StaleIndexReadWrite => {
                        std::fs::write(&shm, &stale_shm).unwrap()
                    }
                    ReadonlyShmShape::RollbackJournalReadWrite => {}
                }
            }
            let stock_before = family(&stock_db);
            let frank_before = family(&frank_db);
            assert_eq!(frank_before, stock_before, "{context}: identical copies");

            let (stock, frank): (ReadThenWrite, ReadThenWrite) =
                if shape == ReadonlyShmShape::MissingShmReadOnly {
                    let query = "mode=ro&readonly_shm=1";
                    (
                        stock_read(&stock_db, query).map(|rows| (rows, None)),
                        frank_read(&frank_db, query).await.map(|rows| (rows, None)),
                    )
                } else {
                    let query = "readonly_shm=1";
                    (
                        stock_read_then_write(&stock_db, query)
                            .map(|(rows, insert)| (rows, Some(insert))),
                        frank_read_then_write(&frank_db, query)
                            .await
                            .map(|(rows, insert)| (rows, Some(insert))),
                    )
                };
            let stock_after = family(&stock_db);
            let frank_after = family(&frank_db);
            eprintln!(
                "event=bd_tv006_readonly_shm shape={shape:?} stock={:?} frank={:?}",
                stock
                    .as_ref()
                    .map(|(rows, insert)| (rows.len(), insert.clone())),
                frank
                    .as_ref()
                    .map(|(rows, insert)| (rows.len(), insert.clone()))
            );

            match shape {
                ReadonlyShmShape::MissingShmReadOnly | ReadonlyShmShape::MissingShmReadWrite => {
                    for (arm, answer) in [("stock", &stock), ("fsqlite", &frank)] {
                        let error = answer.as_ref().err().unwrap_or_else(|| {
                            panic!("{context}: {arm} must refuse, read {answer:?}")
                        });
                        assert!(
                            error.starts_with("unable to open database file"),
                            "{context}: {arm} answers SQLITE_CANTOPEN, got {error}"
                        );
                    }
                    assert_eq!(stock_after, stock_before, "{context}: stock writes nothing");
                    assert_eq!(
                        frank_after, frank_before,
                        "{context}: fsqlite must write nothing"
                    );
                }
                ReadonlyShmShape::StaleIndexReadWrite => {
                    let refused = Ok((
                        expected_rows(),
                        Some(Err("attempt to write a readonly database".to_owned())),
                    ));
                    assert_eq!(
                        stock, refused,
                        "{context}: stock reads, then refuses the write"
                    );
                    assert_eq!(
                        frank, refused,
                        "{context}: fsqlite answers what stock answers"
                    );
                    assert_eq!(stock_after, stock_before, "{context}: stock writes nothing");
                    assert_eq!(
                        frank_after, frank_before,
                        "{context}: readonly_shm=1 must write nothing"
                    );
                }
                ReadonlyShmShape::RollbackJournalReadWrite => {
                    let committed = Ok((expected_rows(), Some(Ok(()))));
                    assert_eq!(stock, committed, "{context}: stock reads and writes");
                    assert_eq!(
                        frank, committed,
                        "{context}: fsqlite answers what stock answers"
                    );
                    assert_eq!(
                        rows_in(&stock_db),
                        ROWS + 1,
                        "{context}: stock's row commits"
                    );
                    assert_eq!(
                        rows_in(&frank_db),
                        ROWS + 1,
                        "{context}: fsqlite's row commits"
                    );
                    // The parameter changes nothing here: each engine leaves
                    // the files it leaves without it.
                    let control_stock = copy("stock-control");
                    let control_frank = copy("frank-control");
                    assert_eq!(
                        stock_read_then_write(&control_stock, "").map(|(rows, _)| rows.len()),
                        Ok(expected_rows().len()),
                        "{context}: the stock control reads"
                    );
                    assert_eq!(
                        frank_read_then_write(&control_frank, "")
                            .await
                            .map(|(rows, _)| rows.len()),
                        Ok(expected_rows().len()),
                        "{context}: the fsqlite control reads"
                    );
                    assert_eq!(
                        stock_after.keys().collect::<Vec<_>>(),
                        family(&control_stock).keys().collect::<Vec<_>>(),
                        "{context}: stock leaves the files its control leaves"
                    );
                    assert_eq!(
                        frank_after.keys().collect::<Vec<_>>(),
                        family(&control_frank).keys().collect::<Vec<_>>(),
                        "{context}: fsqlite leaves the files its control leaves"
                    );
                }
            }
        }
        drop(writer);
    });
}
