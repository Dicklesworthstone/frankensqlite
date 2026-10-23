#![recursion_limit = "512"]

//! bd-reality-check-1022-llk6g.32: `-wal-cert` gained one certificate record
//! per WAL commit and nothing ever removed one, so it grew forever. A WAL
//! reset (new salts, a new generation) makes every older record unreadable,
//! and the first append of the new generation now truncates them away.
//!
//! The keepers check the three things that compaction must not break:
//! the sidecar stays bounded by the current generation's commits, every
//! crash image of the unsynced truncate-then-append reopens with all rows,
//! and a reader in another process never sees a truncate as corruption.

use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const COMMITS_PER_GENERATION: i64 = 20;

fn integer(rows: &[fsqlite_core::connection::Row]) -> i64 {
    match rows[0].values()[0] {
        SqliteValue::Integer(n) => n,
        ref other => panic!("expected an integer, got {other:?}"),
    }
}

async fn scalar(conn: &Connection, sql: &str) -> i64 {
    integer(&conn.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}")))
}

fn sidecar_path(db: &Path) -> PathBuf {
    let mut path = db.as_os_str().to_owned();
    path.push("-wal-cert");
    PathBuf::from(path)
}

fn read_sidecar(db: &Path) -> Vec<u8> {
    std::fs::read(sidecar_path(db)).unwrap_or_default()
}

/// Number of records in the certificate sidecar. Each record ends in a
/// little-endian u32 footer holding its own length.
fn certificate_records(bytes: &[u8]) -> usize {
    let mut records = 0;
    let mut end = bytes.len();
    while end > 0 {
        assert!(end >= 4, "sidecar ends inside a length footer");
        let footer: [u8; 4] = bytes[end - 4..end].try_into().expect("4-byte footer");
        let len = usize::try_from(u32::from_le_bytes(footer)).expect("record length fits usize");
        assert!(len > 4 && len <= end, "invalid record length {len} ending at {end}");
        records += 1;
        end -= len;
    }
    records
}

/// Run a TRUNCATE checkpoint and report whether it completed.
async fn truncate_checkpoint(conn: &Connection) -> bool {
    let rows = conn
        .query("PRAGMA wal_checkpoint(TRUNCATE);")
        .await
        .expect("TRUNCATE checkpoint");
    integer(&rows) == 0
}

#[test]
fn wal_cert_sidecar_stays_bounded_across_checkpoint_resets() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("bounded.db");

    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(db.to_str().expect("utf-8 path"))
            .await
            .expect("open database");
        conn.query("PRAGMA journal_mode = WAL;").await.expect("WAL");
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL);")
            .await
            .expect("create table");
        let mut rows = 0;
        for cycle in 0..5 {
            for _ in 0..COMMITS_PER_GENERATION {
                rows += 1;
                conn.execute(&format!("INSERT INTO t(v) VALUES ('row-{rows}');"))
                    .await
                    .expect("autocommit insert");
            }
            let records = certificate_records(&read_sidecar(&db));
            // Cycle 0 also holds the CREATE TABLE commit.
            assert!(
                records > 0
                    && records
                        <= usize::try_from(COMMITS_PER_GENERATION + 1).expect("fits usize"),
                "cycle {cycle}: -wal-cert holds {records} records after {rows} inserts; \
                 only the current generation's {COMMITS_PER_GENERATION} commits may remain"
            );
            assert!(
                truncate_checkpoint(&conn).await,
                "cycle {cycle}: TRUNCATE checkpoint must complete with no other connection"
            );
        }
        assert_eq!(scalar(&conn, "SELECT count(*) FROM t;").await, rows);
        conn.close().await.expect("close");
    });
}

#[derive(Debug, Clone, Copy)]
enum CompactionCrash {
    /// Both the truncate and the new record reached the disk.
    Intact,
    /// Neither did: the sidecar still holds only the reset generation.
    NothingPersisted,
    /// The truncate did, the new record did not.
    TruncateOnly,
    /// The new record landed over the old bytes but the old size survived,
    /// so the tail is still the reset generation's last record.
    RecordOverOldTail,
}

fn crash_image(
    source_dir: &Path,
    image_dir: &Path,
    before_compaction: &[u8],
    crash: CompactionCrash,
) -> PathBuf {
    std::fs::create_dir_all(image_dir).expect("create crash image dir");
    for suffix in ["", "-wal", "-wal-fec", "-wal-cert-head"] {
        let from = source_dir.join(format!("live.db{suffix}"));
        if from.exists() {
            std::fs::copy(&from, image_dir.join(format!("live.db{suffix}")))
                .expect("copy live file into crash image");
        }
    }
    let after = read_sidecar(&source_dir.join("live.db"));
    let sidecar = match crash {
        CompactionCrash::Intact => after,
        CompactionCrash::NothingPersisted => before_compaction.to_vec(),
        CompactionCrash::TruncateOnly => Vec::new(),
        CompactionCrash::RecordOverOldTail => {
            assert!(
                before_compaction.len() > after.len(),
                "the reset generation left more than one record"
            );
            let mut image = before_compaction.to_vec();
            image[..after.len()].copy_from_slice(&after);
            image
        }
    };
    std::fs::write(image_dir.join("live.db-wal-cert"), sidecar).expect("write sidecar image");
    image_dir.join("live.db")
}

#[test]
fn every_crash_image_of_the_compaction_reopens_with_all_rows() {
    let dir = tempfile::tempdir().expect("tempdir");
    let live = dir.path().join("live");
    std::fs::create_dir_all(&live).expect("live dir");
    let db = live.join("live.db");

    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(db.to_str().expect("utf-8 path"))
            .await
            .expect("open live database");
        conn.query("PRAGMA journal_mode = WAL;").await.expect("WAL");
        conn.execute("PRAGMA synchronous = FULL;").await.expect("FULL");
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL);")
            .await
            .expect("create table");
        for i in 1..=COMMITS_PER_GENERATION {
            conn.execute(&format!("INSERT INTO t(v) VALUES ('row-{i}');"))
                .await
                .expect("autocommit insert");
        }
        assert!(truncate_checkpoint(&conn).await, "TRUNCATE checkpoint");
        let before_compaction = read_sidecar(&db);
        assert!(
            certificate_records(&before_compaction) > 1,
            "the reset generation's records are still on disk before the next append"
        );
        // The first commit of the new generation compacts the sidecar.
        conn.execute("INSERT INTO t(v) VALUES ('first-of-new-generation');")
            .await
            .expect("first insert after the reset");
        let rows = COMMITS_PER_GENERATION + 1;
        assert_eq!(certificate_records(&read_sidecar(&db)), 1);

        for crash in [
            CompactionCrash::Intact,
            CompactionCrash::NothingPersisted,
            CompactionCrash::TruncateOnly,
            CompactionCrash::RecordOverOldTail,
        ] {
            let image = crash_image(
                &live,
                &dir.path().join(format!("{crash:?}")),
                &before_compaction,
                crash,
            );
            let reopened = Connection::open(image.to_str().expect("utf-8 path"))
                .await
                .unwrap_or_else(|e| panic!("{crash:?}: reopen crash image: {e}"));
            assert_eq!(
                scalar(&reopened, "SELECT count(*) FROM t;").await,
                rows,
                "{crash:?}: every FULL-acknowledged row must survive"
            );
            let check = reopened
                .query("PRAGMA integrity_check;")
                .await
                .unwrap_or_else(|e| panic!("{crash:?}: integrity_check: {e}"));
            assert!(
                matches!(&check[0].values()[0], SqliteValue::Text(s) if s.as_ref() == "ok"),
                "{crash:?}: integrity_check returned {:?}",
                check[0].values()
            );
            reopened
                .execute("INSERT INTO t(v) VALUES ('after-reopen');")
                .await
                .unwrap_or_else(|e| panic!("{crash:?}: write after reopen: {e}"));
            assert_eq!(scalar(&reopened, "SELECT count(*) FROM t;").await, rows + 1);
            let records = certificate_records(&read_sidecar(&image));
            assert!(
                (1..=2).contains(&records),
                "{crash:?}: only current-generation records may remain, found {records}"
            );
            reopened
                .close()
                .await
                .unwrap_or_else(|e| panic!("{crash:?}: close: {e}"));

            let stock = rusqlite::Connection::open(&image).expect("stock open");
            let count: i64 = stock
                .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
                .expect("stock count");
            assert_eq!(count, rows + 1, "{crash:?}: stock row count");
            let integrity: String = stock
                .query_row("PRAGMA integrity_check", [], |row| row.get(0))
                .expect("stock integrity_check");
            assert_eq!(integrity, "ok", "{crash:?}: stock integrity_check");
        }
        conn.close().await.expect("close live connection");
    });
}

const PEER_ENV: &str = "FSQLITE_WAL_CERT_COMPACTION_PEER_DB";
const PEER_TEST: &str = "peer_reader_never_sees_a_compaction_as_corruption";
const READY: &str = "WAL_CERT_PEER_READY";
const DONE: &str = "WAL_CERT_PEER_DONE";

/// Peer mode: read in a loop until stdin closes, then report how many reads
/// ran and how many failed as corruption.
fn run_peer_reader(db: &str) {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_on_eof = Arc::clone(&stop);
    std::thread::spawn(move || {
        let mut sink = Vec::new();
        let _ = std::io::stdin().read_to_end(&mut sink);
        stop_on_eof.store(true, Ordering::SeqCst);
    });
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(db).await.expect("peer open");
        conn.execute("PRAGMA busy_timeout = 5000;").await.expect("peer busy timeout");
        println!("{READY}");
        std::io::stdout().flush().expect("flush readiness");
        let mut reads = 0_u64;
        let mut corrupt = 0_u64;
        let mut first_corruption = String::new();
        while !stop.load(Ordering::SeqCst) {
            match conn.query("SELECT count(*) FROM t;").await {
                Ok(_) => reads += 1,
                Err(error) => {
                    let message = error.to_string();
                    let lower = message.to_lowercase();
                    if lower.contains("corrupt") || lower.contains("malformed") {
                        corrupt += 1;
                        if first_corruption.is_empty() {
                            first_corruption = message;
                        }
                    }
                }
            }
        }
        println!("{DONE} {reads} {corrupt} {first_corruption}");
        std::io::stdout().flush().expect("flush report");
        conn.close().await.expect("peer close");
    });
}

#[test]
fn peer_reader_never_sees_a_compaction_as_corruption() {
    if let Some(db) = std::env::var_os(PEER_ENV) {
        run_peer_reader(&db.into_string().expect("utf-8 peer path"));
        return;
    }

    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("shared.db");
    let db_str = db.to_str().expect("utf-8 path").to_owned();

    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(&db_str).await.expect("open writer");
        conn.query("PRAGMA journal_mode = WAL;").await.expect("WAL");
        conn.execute("PRAGMA busy_timeout = 5000;").await.expect("writer busy timeout");
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL);")
            .await
            .expect("create table");
        conn.execute("INSERT INTO t(v) VALUES ('seed');").await.expect("seed");

        let mut peer = Command::new(std::env::current_exe().expect("test binary"))
            .args([PEER_TEST, "--exact", "--nocapture", "--test-threads=1"])
            .env(PEER_ENV, &db_str)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn peer reader");
        let peer_stdin = peer.stdin.take().expect("peer stdin");
        let mut peer_stdout = BufReader::new(peer.stdout.take().expect("peer stdout"));
        let started = Instant::now();
        loop {
            let mut line = String::new();
            let read = peer_stdout.read_line(&mut line).expect("read peer stdout");
            assert!(read > 0, "peer exited before reporting readiness");
            // libtest writes "test <name> ... " with no newline before the
            // body runs, so readiness can share that line.
            if line.contains(READY) {
                break;
            }
            assert!(started.elapsed() < Duration::from_secs(60), "peer never became ready");
        }

        let mut resets = 0;
        for cycle in 0..40 {
            for i in 0..5 {
                conn.execute(&format!("INSERT INTO t(v) VALUES ('c{cycle}-{i}');"))
                    .await
                    .unwrap_or_else(|e| panic!("cycle {cycle}: insert: {e}"));
            }
            if truncate_checkpoint(&conn).await {
                resets += 1;
            }
        }
        drop(peer_stdin);
        let mut report = None;
        loop {
            let mut line = String::new();
            if peer_stdout.read_line(&mut line).expect("read peer report") == 0 {
                break;
            }
            if let Some(at) = line.find(DONE) {
                report = Some(line[at + DONE.len()..].trim().to_owned());
            }
        }
        let status = peer.wait().expect("wait for peer");
        let report = report.unwrap_or_else(|| panic!("peer exited ({status}) without a report"));
        let mut fields = report.splitn(3, ' ');
        let reads: u64 = fields.next().and_then(|s| s.parse().ok()).expect("read count");
        let corrupt: u64 = fields.next().and_then(|s| s.parse().ok()).expect("corrupt count");
        let first = fields.next().unwrap_or_default();
        assert!(status.success(), "peer reader failed: {status}");
        assert!(resets > 0, "no TRUNCATE checkpoint completed, so nothing was compacted");
        assert!(reads > 0, "the peer never completed a read");
        assert_eq!(
            corrupt, 0,
            "the peer saw {corrupt} of {reads} reads fail as corruption across {resets} \
             compactions; first: {first}"
        );
        conn.close().await.expect("close writer");
    });
}
