//! GH#528: stock SQLite's first connection to attach to a WAL database
//! truncates `-shm` to 3 bytes and only rebuilds the index afterwards, under
//! its WAL locks, so one that dies in between leaves an index nobody
//! initialized. A plain FrankenSQLite read-only open could not rebuild it and
//! answered `BusyRecovery` forever, even with no process holding the database,
//! so beads_rust (`br`), which reads through such an open first, could no
//! longer use its own tracker.
//!
//! Expected, matching stock SQLite:
//! - a plain read-only open rebuilds an existing, uninitialized index (a
//!   `-shm` shorter than one region, or a primary header copy that was never
//!   initialized) under every WAL recovery lock, reads every committed row,
//!   and leaves the WAL byte-identical;
//! - while another process holds one of those locks it fails with a retryable
//!   busy error and changes nothing, then succeeds once the lock is released,
//!   even with that process still attached;
//! - it never rewrites an initialized index, and never creates a `-shm`;
//! - stock SQLite reads the result, including a stock connection that stayed
//!   attached through the rebuild.
//!
//! The foreign connections are child processes: POSIX locks are per process,
//! so an in-process stock connection would share FrankenSQLite's locks.

#![cfg(all(unix, feature = "native"))]
// The async engine futures nest deeply; match the other integration suites.
#![recursion_limit = "512"]

use std::io::{BufRead as _, Write as _};
use std::os::unix::fs::FileExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;
use fsqlite_wal::wal_index::{WAL_INDEX_VERSION, parse_shm_header};
use fsqlite_wal::{WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, WalHeader};

const ROWS: i64 = 40;
/// One WAL-index region; stock and FrankenSQLite grow `-shm` in these.
const SEGMENT: u64 = 32 * 1024;
/// Byte offsets of `isInit` and `mxFrame` in header copies 1 and 2.
const IS_INIT_OFFSETS: [u64; 2] = [12, 60];
const MX_FRAME_OFFSETS: [u64; 2] = [16, 64];
/// The bytes after the header and checkpoint information hold index entries.
const ENTRIES_OFFSET: u64 = 136;
/// SQLite's WAL WRITE lock byte in `-shm` (lock bytes start at 120).
const WAL_WRITE_LOCK_BYTE: i64 = 120;

const CHILD_TEST: &str = "gh528_child_process";
const CHILD_ROLE_ENV: &str = "FSQLITE_GH528_CHILD_ROLE";
const CHILD_DB_ENV: &str = "FSQLITE_GH528_CHILD_DB";
const WITNESS: &str = "gh528:";

fn sidecar(db: &Path, suffix: &str) -> PathBuf {
    let mut path = db.as_os_str().to_owned();
    path.push(suffix);
    PathBuf::from(path)
}

fn value(id: i64) -> String {
    format!("row-{id}-{}", "x".repeat(200))
}

fn expected_rows() -> Vec<(i64, String)> {
    (1..=ROWS).map(|id| (id, value(id))).collect()
}

async fn read_rows(conn: &Connection) -> Result<Vec<(i64, String)>, FrankenError> {
    let rows = conn.query("SELECT id, v FROM t ORDER BY id").await?;
    Ok(rows
        .iter()
        .map(|row| match row.values() {
            [SqliteValue::Integer(id), SqliteValue::Text(v)] => (*id, v.to_string()),
            other => panic!("unexpected row {other:?}"),
        })
        .collect())
}

/// A plain read-only open, the way `br`'s read-only probe reads.
async fn plain_read_only_rows(db: &Path) -> Result<Vec<(i64, String)>, FrankenError> {
    let conn = Connection::open_schema_only(db.to_str().expect("utf-8 path")).await?;
    let rows = read_rows(&conn).await;
    drop(conn);
    rows
}

async fn write_rows(db: &Path, settle: bool) {
    let conn = Connection::open(db.to_str().expect("utf-8 path"))
        .await
        .expect("open writer");
    conn.execute("PRAGMA journal_mode=WAL").await.expect("wal");
    conn.execute("PRAGMA wal_autocheckpoint=0")
        .await
        .expect("no autocheckpoint");
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL)")
        .await
        .expect("create");
    for id in 1..=ROWS {
        conn.execute(&format!("INSERT INTO t VALUES ({id}, '{}')", value(id)))
            .await
            .expect("insert");
    }
    if settle {
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            .await
            .expect("settle the WAL");
    }
    conn.close_without_checkpoint()
        .await
        .expect("close without checkpoint");
}

/// Leave `db` with its schema and every row only in committed WAL frames,
/// beside the valid index the writer published. Returns the WAL bytes.
async fn stage_wal_frames(db: &Path) -> Vec<u8> {
    write_rows(db, false).await;
    let wal = std::fs::read(sidecar(db, "-wal")).expect("the WAL survives the close");
    assert!(
        wal_frames(&wal) > 0,
        "the committed rows are still in the WAL"
    );
    let shm = std::fs::read(sidecar(db, "-shm")).expect("the -shm survives the close");
    assert!(
        parse_shm_header(&shm).expect("parse -shm").is_some(),
        "the writer left a valid index"
    );
    wal
}

fn wal_frames(wal: &[u8]) -> u32 {
    if wal.len() <= WAL_HEADER_SIZE {
        return 0;
    }
    let header = WalHeader::from_bytes(wal).expect("WAL header");
    let frame = WAL_FRAME_HEADER_SIZE + usize::try_from(header.page_size).expect("page size");
    u32::try_from((wal.len() - WAL_HEADER_SIZE) / frame).expect("frame count")
}

fn open_shm(db: &Path) -> std::fs::File {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(sidecar(db, "-shm"))
        .expect("open -shm")
}

/// Stock's first attacher: `ftruncate(-shm, 3)`, which keeps the first three
/// bytes of `iVersion`, and nothing more. `len` varies the cut.
fn truncate_shm(db: &Path, len: u64) {
    open_shm(db).set_len(len).expect("truncate -shm");
}

fn store_shm(db: &Path, offset: u64, bytes: &[u8]) {
    open_shm(db)
        .write_all_at(bytes, offset)
        .expect("write -shm");
}

fn read_shm(db: &Path) -> Vec<u8> {
    std::fs::read(sidecar(db, "-shm")).expect("read -shm")
}

/// A full-region `-shm` whose accepted header names the WAL's last frame, as
/// stock's `walIndexRecover` leaves it.
fn assert_rebuilt_index(db: &Path, wal: &[u8], context: &str) {
    let shm = read_shm(db);
    let len = u64::try_from(shm.len()).expect("length");
    assert!(
        len >= SEGMENT && len.is_multiple_of(SEGMENT),
        "{context}: -shm is {len} bytes"
    );
    let (header, _) = parse_shm_header(&shm)
        .expect("parse -shm")
        .unwrap_or_else(|| panic!("{context}: the rebuilt -shm has no valid header"));
    assert_eq!(header.mx_frame, wal_frames(wal), "{context}: mxFrame");
}

/// Stock reads every row and finds the database intact.
fn stock_rows(db: &Path) -> Vec<(i64, String)> {
    let stock =
        rusqlite::Connection::open_with_flags(db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("stock read-only open");
    let check: String = stock
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("stock integrity_check");
    assert_eq!(check, "ok");
    stock
        .prepare("SELECT id, v FROM t ORDER BY id")
        .expect("prepare")
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .expect("query")
        .collect::<rusqlite::Result<_>>()
        .expect("stock rows")
}

/// The whole recovery: rows, an untouched WAL, an accepted index that a
/// second plain read-only open leaves byte-identical, and stock agreement.
async fn assert_read_only_recovers(db: &Path, wal: &[u8], context: &str) {
    let started = Instant::now();
    let rows = plain_read_only_rows(db)
        .await
        .unwrap_or_else(|error| panic!("{context}: plain read-only open failed: {error:?}"));
    assert_eq!(rows, expected_rows(), "{context}");
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "{context}: recovery took {:?}",
        started.elapsed()
    );
    assert_eq!(
        std::fs::read(sidecar(db, "-wal")).expect("read WAL"),
        wal,
        "{context}: the WAL is untouched"
    );
    assert_rebuilt_index(db, wal, context);
    let rebuilt = read_shm(db);
    assert_eq!(
        plain_read_only_rows(db).await.expect("second read"),
        expected_rows()
    );
    assert_eq!(
        read_shm(db),
        rebuilt,
        "{context}: a valid index is never rewritten"
    );
    assert_eq!(stock_rows(db), expected_rows(), "{context}: stock agrees");
}

#[test]
fn plain_read_only_open_rebuilds_a_truncated_shm_beside_wal_frames() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        for len in [3, 0, 100, SEGMENT - 1] {
            let db = dir.path().join(format!("frames-{len}.db"));
            let wal = stage_wal_frames(&db).await;
            truncate_shm(&db, len);
            if len == 3 {
                assert_eq!(
                    read_shm(&db),
                    WAL_INDEX_VERSION.to_ne_bytes()[..3],
                    "stock's marker"
                );
            }
            assert_read_only_recovers(&db, &wal, &format!("{len}-byte -shm")).await;
        }
    });
}

#[test]
fn plain_read_only_open_rebuilds_a_truncated_shm_beside_a_frameless_wal() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        for shape in ["header-only", "empty"] {
            let db = dir.path().join(format!("{shape}.db"));
            write_rows(&db, true).await;
            let wal = if shape == "header-only" {
                WalHeader {
                    magic: fsqlite_wal::WAL_MAGIC_LE,
                    format_version: fsqlite_wal::WAL_FORMAT_VERSION,
                    page_size: 4096,
                    checkpoint_seq: 3,
                    salts: fsqlite_wal::WalSalts {
                        salt1: 0x5280_0528,
                        salt2: 0x0bad_f00d,
                    },
                    checksum: fsqlite_wal::SqliteWalChecksum { s1: 0, s2: 0 },
                }
                .to_bytes()
                .expect("encode WAL header")
                .to_vec()
            } else {
                Vec::new()
            };
            std::fs::write(sidecar(&db, "-wal"), &wal).expect("shape the WAL");
            truncate_shm(&db, 3);
            let rows = plain_read_only_rows(&db)
                .await
                .unwrap_or_else(|error| panic!("{shape}: plain read-only open failed: {error:?}"));
            assert_eq!(rows, expected_rows(), "{shape}");
            assert_eq!(
                std::fs::read(sidecar(&db, "-wal")).expect("WAL"),
                wal,
                "{shape}"
            );
            if shape == "header-only" {
                assert_rebuilt_index(&db, &wal, shape);
            }
            assert_eq!(stock_rows(&db), expected_rows(), "{shape}: stock agrees");
        }
    });
}

#[test]
fn writable_open_rebuilds_a_truncated_shm_and_keeps_committing() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("writable.db");
        stage_wal_frames(&db).await;
        truncate_shm(&db, 3);
        let writer = Connection::open(db.to_str().expect("utf-8 path"))
            .await
            .expect("writable open");
        assert_eq!(read_rows(&writer).await.expect("rows"), expected_rows());
        writer
            .execute("INSERT INTO t VALUES (41, 'after the rebuild')")
            .await
            .expect("commit after the rebuild");
        writer
            .close_without_checkpoint()
            .await
            .expect("close writer");

        let mut expected = expected_rows();
        expected.push((ROWS + 1, "after the rebuild".to_owned()));
        let rows = plain_read_only_rows(&db).await.expect("read-only open");
        assert_eq!(rows, expected);
        assert_eq!(stock_rows(&db), expected, "stock agrees");
    });
}

#[test]
fn plain_read_only_open_rebuilds_an_interrupted_reconstruction() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        for shape in [
            "stock-extended",
            "invalidated-mid-rebuild",
            "copy-two-published",
        ] {
            let db = dir.path().join(format!("{shape}.db"));
            let wal = stage_wal_frames(&db).await;
            let valid = read_shm(&db);
            match shape {
                // Truncated to stock's marker, then extended with zeros
                // before any header store: the copies differ like a torn pair.
                "stock-extended" => {
                    truncate_shm(&db, 3);
                    truncate_shm(&db, SEGMENT);
                }
                // A rebuild that invalidated both copies and stopped part way
                // through rewriting entries, which now name the wrong pages.
                "invalidated-mid-rebuild" => {
                    for offset in IS_INIT_OFFSETS {
                        store_shm(&db, offset, &[0; 4]);
                    }
                    let garbage = vec![0x5a; usize::try_from(SEGMENT - ENTRIES_OFFSET).unwrap()];
                    store_shm(&db, ENTRIES_OFFSET, &garbage);
                }
                // A rebuild that stored copy 2 and stopped before copy 1.
                "copy-two-published" => {
                    store_shm(&db, IS_INIT_OFFSETS[0], &[0; 4]);
                    store_shm(&db, 48, &valid[..48]);
                }
                _ => unreachable!(),
            }
            assert!(
                parse_shm_header(&read_shm(&db)).expect("parse").is_none(),
                "{shape}"
            );
            assert_read_only_recovers(&db, &wal, shape).await;
        }
    });
}

#[test]
fn plain_read_only_open_never_rewrites_an_initialized_index_or_creates_shm() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");

        // Initialized but invalid: both copies agree on an mxFrame that their
        // checksum does not cover. Only a connection allowed to rewrite a
        // live-looking index may repair it.
        let db = dir.path().join("initialized.db");
        let wal = stage_wal_frames(&db).await;
        for offset in MX_FRAME_OFFSETS {
            store_shm(&db, offset, &(wal_frames(&wal) + 7).to_ne_bytes());
        }
        let before = read_shm(&db);
        let error = plain_read_only_rows(&db)
            .await
            .expect_err("an initialized invalid index is not rebuilt read-only");
        assert!(matches!(error, FrankenError::BusyRecovery), "got {error:?}");
        assert_eq!(read_shm(&db), before, "the initialized index is left alone");
        let recovering =
            Connection::open_schema_only_with_wal_index_recovery(db.to_str().expect("utf-8"))
                .await
                .expect("the explicit recovery mode still rebuilds it");
        assert_eq!(read_rows(&recovering).await.expect("rows"), expected_rows());
        drop(recovering);
        assert_rebuilt_index(&db, &wal, "explicit recovery");

        // Missing: read-only opens create no sidecar, and never read wrong rows.
        let db = dir.path().join("missing.db");
        stage_wal_frames(&db).await;
        std::fs::remove_file(sidecar(&db, "-shm")).expect("remove -shm");
        if let Ok(rows) = plain_read_only_rows(&db).await {
            assert_eq!(rows, expected_rows());
        }
        assert!(
            !sidecar(&db, "-shm").exists(),
            "a read-only open created -shm"
        );
    });
}

#[test]
fn read_only_rebuild_waits_for_a_stock_reader_and_serves_it_afterwards() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("live-stock-peer.db");
        let wal = stage_wal_frames(&db).await;

        // A live stock connection attaches first, so it truncates and
        // rebuilds the index itself, then pins a read snapshot.
        let mut stock = ChildProcess::spawn("stock-reader", &db);
        stock.signal("pin");
        assert_eq!(stock.witness("pinned"), ROWS.to_string());
        // The index then loses its initialization while that reader pins it,
        // as if a recovery had begun and died.
        for offset in IS_INIT_OFFSETS {
            store_shm(&db, offset, &[0; 4]);
        }
        let uninitialized = read_shm(&db);

        // Exclusive recovery authority is unavailable: fail retryably and
        // change nothing.
        let error = plain_read_only_rows(&db)
            .await
            .expect_err("a pinned reader blocks the rebuild");
        assert!(
            matches!(error, FrankenError::Busy | FrankenError::BusyRecovery),
            "expected a retryable busy, got {error:?}"
        );
        assert!(error.is_transient(), "{error:?} must invite a retry");
        assert_eq!(
            read_shm(&db),
            uninitialized,
            "a refused rebuild changes nothing"
        );

        // Released but still attached: the rebuild proceeds, and the stock
        // peer reads through the index FrankenSQLite rebuilt.
        stock.signal("release");
        stock.witness("released");
        assert_eq!(
            plain_read_only_rows(&db).await.expect("read after release"),
            expected_rows()
        );
        assert_rebuilt_index(&db, &wal, "rebuilt beside a live stock peer");
        stock.signal("count");
        assert_eq!(stock.witness("count"), ROWS.to_string());
        stock.signal("exit");
        stock.wait_success();
        assert_eq!(
            plain_read_only_rows(&db).await.expect("read after exit"),
            expected_rows()
        );
    });
}

#[test]
fn read_only_open_recovers_after_a_killed_stock_first_attacher() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("killed-stock.db");
        let wal = stage_wal_frames(&db).await;

        // The reported state, produced by stock itself: hold WRITE so stock's
        // first attacher truncates `-shm` and then cannot recover, and kill it
        // there.
        let mut holder = ChildProcess::spawn("hold-write", &db);
        holder.witness("holding");
        let mut stock = ChildProcess::spawn("stock-first-attacher", &db);
        let deadline = Instant::now() + Duration::from_secs(30);
        while std::fs::metadata(sidecar(&db, "-shm")).expect("-shm").len() != 3 {
            assert!(Instant::now() < deadline, "stock never truncated -shm");
            std::thread::sleep(Duration::from_millis(5));
        }
        stock.kill();
        holder.signal("exit");
        holder.wait_success();
        assert_eq!(
            read_shm(&db),
            WAL_INDEX_VERSION.to_ne_bytes()[..3],
            "stock's marker"
        );

        assert_read_only_recovers(&db, &wal, "killed stock first attacher").await;
    });
}

#[test]
fn concurrent_read_only_attachers_all_read_the_rebuilt_index() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");

        // Separate processes, released together.
        let db = dir.path().join("processes.db");
        let wal = stage_wal_frames(&db).await;
        truncate_shm(&db, 3);
        let mut readers: Vec<_> = (0..4)
            .map(|_| ChildProcess::spawn("frank-reader", &db))
            .collect();
        for reader in &mut readers {
            reader.witness("ready");
        }
        for reader in &mut readers {
            reader.signal("go");
        }
        for reader in &mut readers {
            assert_eq!(reader.witness("rows"), ROWS.to_string());
            reader.wait_success();
        }
        assert_rebuilt_index(&db, &wal, "concurrent processes");
        assert_eq!(stock_rows(&db), expected_rows());

        // Separate connections in one process. Recovery needs sole
        // same-process ownership, so openers that all stay counted while
        // they retry would starve each other without the rebuild drain. The
        // stock-extended shape holds every attempt through its torn-header
        // retries, the widest such overlap.
        for shm_len in [3, SEGMENT] {
            let db = dir.path().join(format!("threads-{shm_len}.db"));
            let wal = stage_wal_frames(&db).await;
            truncate_shm(&db, 3);
            truncate_shm(&db, shm_len);
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
            let threads: Vec<_> = (0..8)
                .map(|_| {
                    let db = db.clone();
                    let barrier = std::sync::Arc::clone(&barrier);
                    std::thread::spawn(move || {
                        asupersync::test_utils::run_test(|| async move {
                            barrier.wait();
                            assert_eq!(read_with_busy_retry(&db).await, expected_rows());
                        });
                    })
                })
                .collect();
            for thread in threads {
                thread.join().expect("reader thread");
            }
            assert_rebuilt_index(&db, &wal, &format!("{shm_len}-byte concurrent connections"));
        }
    });
}

/// Retry a busy refusal the way a caller with a busy timeout would. Another
/// attacher holding the recovery locks clears in milliseconds.
async fn read_with_busy_retry(db: &Path) -> Vec<(i64, String)> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match plain_read_only_rows(db).await {
            Ok(rows) => return rows,
            Err(error) if error.is_transient() && Instant::now() < deadline => {
                asupersync::time::sleep(asupersync::time::wall_now(), Duration::from_millis(2))
                    .await;
            }
            Err(error) => panic!("plain read-only open failed: {error:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Child processes.

/// Dispatch a child role; a no-op when run as an ordinary test.
#[test]
fn gh528_child_process() {
    let (Some(role), Some(db)) = (
        std::env::var_os(CHILD_ROLE_ENV),
        std::env::var_os(CHILD_DB_ENV),
    ) else {
        return;
    };
    let db = PathBuf::from(db);
    match role.to_str().expect("utf-8 role") {
        "stock-reader" => run_stock_reader(&db),
        "stock-first-attacher" => {
            let stock = rusqlite::Connection::open(&db).expect("stock open");
            // Spins in stock's WAL_RETRY loop until the parent kills it.
            let _ = stock.query_row("SELECT count(*) FROM t", [], |row| row.get::<_, i64>(0));
        }
        "hold-write" => hold_write_lock(&db),
        "frank-reader" => {
            witness("ready", "");
            expect_line("go");
            asupersync::test_utils::run_test(|| async {
                let rows = read_with_busy_retry(&db).await;
                assert_eq!(rows, expected_rows());
                witness("rows", &rows.len().to_string());
            });
        }
        other => panic!("unknown child role {other}"),
    }
}

fn witness(tag: &str, value: &str) {
    println!("{WITNESS}{tag} {value}");
    std::io::stdout().flush().expect("flush witness");
}

fn expect_line(expected: &str) {
    let mut line = String::new();
    std::io::stdin()
        .lock()
        .read_line(&mut line)
        .expect("child reads a control line");
    assert_eq!(line.trim(), expected, "child control protocol");
}

fn run_stock_reader(db: &Path) {
    let stock = rusqlite::Connection::open(db).expect("stock open");
    let count = |stock: &rusqlite::Connection| -> i64 {
        stock
            .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
            .expect("stock count")
    };
    expect_line("pin");
    stock.execute_batch("BEGIN").expect("begin");
    witness("pinned", &count(&stock).to_string());
    expect_line("release");
    stock.execute_batch("COMMIT").expect("end the read");
    witness("released", "");
    expect_line("count");
    witness("count", &count(&stock).to_string());
    expect_line("exit");
}

/// Hold the WAL WRITE lock byte like a stock writer would, without attaching.
fn hold_write_lock(db: &Path) {
    use nix::fcntl::{FcntlArg, fcntl};
    use nix::libc;
    let shm = open_shm(db);
    #[allow(
        clippy::useless_conversion,
        reason = "F_WRLCK is a c_short on Apple targets but a c_int on Linux"
    )]
    let lock = libc::flock {
        l_type: i16::try_from(libc::F_WRLCK).expect("lock type"),
        l_whence: i16::try_from(libc::SEEK_SET).expect("whence"),
        l_start: WAL_WRITE_LOCK_BYTE,
        l_len: 1,
        l_pid: 0,
    };
    fcntl(&shm, FcntlArg::F_SETLK(&lock)).expect("take the WAL WRITE lock");
    witness("holding", "");
    expect_line("exit");
    drop(shm);
}

/// One child running [`gh528_child_process`], killed if still alive on drop.
struct ChildProcess {
    child: std::process::Child,
    output: Receiver<String>,
    role: &'static str,
}

impl ChildProcess {
    fn spawn(role: &'static str, db: &Path) -> Self {
        let mut child = Command::new(std::env::current_exe().expect("test binary"))
            .args([CHILD_TEST, "--exact", "--nocapture", "--test-threads=1"])
            .env(CHILD_ROLE_ENV, role)
            .env(CHILD_DB_ENV, db.as_os_str())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn child");
        let stdout = child.stdout.take().expect("child stdout");
        let (sender, output) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in std::io::BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                // The harness's `test ... ` banner may precede the first one.
                if let Some((_, witness)) = line.split_once(WITNESS)
                    && sender.send(witness.to_owned()).is_err()
                {
                    break;
                }
            }
        });
        Self {
            child,
            output,
            role,
        }
    }

    fn signal(&mut self, line: &str) {
        let stdin = self.child.stdin.as_mut().expect("child stdin");
        writeln!(stdin, "{line}").expect("signal child");
        stdin.flush().expect("flush child stdin");
    }

    /// The value of the child's next `tag` witness.
    fn witness(&self, tag: &str) -> String {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let line = self.output.recv_timeout(remaining).unwrap_or_else(|error| {
                panic!("{} child produced no {tag} witness: {error}", self.role)
            });
            if let Some(value) = line.strip_prefix(tag) {
                return value.trim().to_owned();
            }
        }
    }

    fn kill(&mut self) {
        self.child.kill().expect("kill child");
        self.child.wait().expect("reap child");
    }

    fn wait_success(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        let status = loop {
            if let Some(status) = self.child.try_wait().expect("poll child") {
                break status;
            }
            assert!(
                Instant::now() < deadline,
                "{} child did not exit",
                self.role
            );
            std::thread::sleep(Duration::from_millis(5));
        };
        assert!(status.success(), "{} child exited with {status}", self.role);
    }
}

impl Drop for ChildProcess {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}
