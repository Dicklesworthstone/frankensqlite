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
    let db = dir.join("source.db");
    let writer = rusqlite::Connection::open(&db).unwrap();
    writer
        .execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
             CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL);",
        )
        .unwrap();
    for (id, v) in expected_rows() {
        writer
            .execute("INSERT INTO t VALUES (?1, ?2)", rusqlite::params![id, v])
            .unwrap();
        if id == MAIN_FILE_ROWS {
            writer
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
                .unwrap();
        }
    }
    (writer, db)
}

fn copy_family(source: &Path, to: &Path) -> PathBuf {
    std::fs::create_dir_all(to).unwrap();
    let db = to.join("copy.db");
    for suffix in ["", "-wal", "-shm"] {
        std::fs::copy(sidecar(source, suffix), sidecar(&db, suffix)).unwrap();
    }
    db
}

/// Child: a stock SQLite read through `file:<db>?<query>`, as witness lines.
#[test]
fn bd_j2dtf_stock_child() {
    let (Some(db), Some(query)) = (
        std::env::var_os(STOCK_DB_ENV),
        std::env::var_os(STOCK_QUERY_ENV),
    ) else {
        return;
    };
    let uri = format!("file:{}?{}", db.to_str().unwrap(), query.to_str().unwrap());
    let read = || -> rusqlite::Result<Rows> {
        let conn = rusqlite::Connection::open_with_flags(
            &uri,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        )?;
        let rows = conn
            .prepare("SELECT id, v FROM t ORDER BY id")?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<Rows>>()?;
        Ok(rows)
    };
    match read() {
        Ok(rows) => {
            for (id, v) in rows {
                println!("{STOCK_WITNESS}row {id} {v}");
            }
            println!("{STOCK_WITNESS}ok");
        }
        Err(error) => println!("{STOCK_WITNESS}error {error}"),
    }
}

/// The stock arm: a separate process, so its POSIX locks are its own.
fn stock_read(db: &Path, query: &str) -> Result<Rows, String> {
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            STOCK_CHILD_TEST,
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(STOCK_DB_ENV, db)
        .env(STOCK_QUERY_ENV, query)
        .output()
        .expect("run the stock child");
    assert!(output.status.success(), "stock child failed: {output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    let mut rows = Vec::new();
    for line in stdout.lines() {
        // The harness's `test ... ` banner may precede the first witness.
        let Some((_, witness)) = line.split_once(STOCK_WITNESS) else {
            continue;
        };
        if let Some(row) = witness.strip_prefix("row ") {
            let (id, v) = row.split_once(' ').unwrap();
            rows.push((id.parse().unwrap(), v.to_owned()));
        } else if witness == "ok" {
            return Ok(rows);
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReadOnlyShape {
    /// Stock's first-attacher marker left by one that died (GH#528).
    ShmThreeBytes,
    /// A full region nobody initialized.
    ShmZeroFilled,
    /// A valid index the reader may not write (GH#535's comment).
    ShmValidReadOnly,
    /// `immutable=1`: no locks, no `-wal`, no `-shm`.
    Immutable,
}

/// bd-j2dtf: a read-only open beside a live stock process's answer for the
/// same file family. Stock (here the bundled 3.54.1 in a child process; the
/// bead also measured 3.46.1) rebuilds a short or uninitialized writable
/// `-shm`, reads every row and leaves the main file and the WAL unchanged.
/// FrankenSQLite must give the same rows and the same rebuilt index. A
/// read-only `-shm` and `immutable=1` are not implemented yet (bd-j2dtf
/// items 2 and 3): for them FrankenSQLite may refuse, but it may never answer
/// differently from stock or write anything.
#[test]
fn read_only_opens_match_a_live_stock_process() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let (writer, source) = stock_fixture(dir.path());
        for shape in [
            ReadOnlyShape::ShmThreeBytes,
            ReadOnlyShape::ShmZeroFilled,
            ReadOnlyShape::ShmValidReadOnly,
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
                    ReadOnlyShape::ShmThreeBytes => {
                        std::fs::OpenOptions::new()
                            .write(true)
                            .open(&shm)
                            .unwrap()
                            .set_len(3)
                            .unwrap();
                    }
                    ReadOnlyShape::ShmZeroFilled => {
                        std::fs::write(&shm, vec![0_u8; SHM_REGION]).unwrap();
                    }
                    ReadOnlyShape::ShmValidReadOnly => {
                        std::fs::set_permissions(&shm, std::fs::Permissions::from_mode(0o444))
                            .unwrap();
                    }
                    ReadOnlyShape::Immutable => {}
                }
            }
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
                ReadOnlyShape::ShmValidReadOnly | ReadOnlyShape::Immutable => {
                    assert_eq!(stock_after, before, "{context}: stock writes nothing");
                    assert_eq!(frank_after, before, "{context}: fsqlite must write nothing");
                    if let Ok(rows) = frank {
                        assert_eq!(rows, stock_rows, "{context}: never a different answer");
                    }
                }
            }
        }
        drop(writer);
    });
}
