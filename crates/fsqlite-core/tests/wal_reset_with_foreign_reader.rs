//! GH #411 keeper — a committed transaction must not be destroyed when a
//! FOREIGN (stock-SQLite, separate process) connection closes its handle on
//! the same WAL-mode database.
//!
//! # The original bug
//!
//! fsqlite appended committed frames to `-wal` without advancing `mxFrame`
//! in the shared `-shm` WAL-index or retaining a main-file SHARED claim for
//! the WAL attachment. That allowed this sequence:
//!
//! 1. every attached stock connection still reads the pre-commit `mxFrame` and
//!    cannot see the commit (GH #19 — `write_shm_header` and
//!    `update_legacy_shm` have no production callers); and
//! 2. stock's `sqlite3WalClose` succeeds in taking an EXCLUSIVE lock on the
//!    main file, concludes it is the last connection, checkpoints only up to
//!    that stale `mxFrame`, and unlinks `-wal`/`-shm`.
//!
//! The committed frames go with it. `execute` returned `Ok`, the writer's own
//! `SELECT` returned the new row, and then the row vanishes — for fsqlite as
//! well as for stock — because an unrelated process closed a handle.
//!
//! Reported from `Dicklesworthstone/mcp_agent_mail_rust`, where it is the
//! remaining failure in
//! `queries::tests::commit_tx_does_not_wait_for_external_reader_checkpoint`
//! (`crates/mcp-agent-mail-db`): a write through the fsqlite pool returns `Ok`
//! while an external canonical reader holds `BEGIN; SELECT …`, and the row is
//! afterwards absent, with `-wal` back to a bare 32-byte header and a passive
//! checkpoint reporting 0 frames published.
//!
//! # Why the reader is a child PROCESS
//!
//! POSIX advisory locks are per-process. An in-process stock reader shares
//! fsqlite's lock ownership and never exercises the cross-process WAL close
//! protocol this bug lives in — with an in-process reader the scenario passes.
//!
//! # Status
//!
//! The Unix VFS now retains an independent SHM-lifetime main-file read claim.
//! This keeper runs by default and also verifies fresh stock/fsqlite reopen.
//! GH #19 shared-index publication remains a separate open issue: this test
//! does not require an already-attached stock reader to see the new commit.
//! GitHub Actions is disabled; run the integration target explicitly:
//!
//! ```text
//! cargo test -p fsqlite-core --test wal_reset_with_foreign_reader -- --nocapture
//! ```
//!
//! The `eprintln!` probes are deliberate: `(mxFrame, nBackfill, aReadMark)`
//! read straight out of `-shm` plus the `-wal` length at each step is what
//! separates the two defects, and a future regression will want them.

use std::io::{BufRead as _, Write as _};
use std::path::Path;
use std::process::Stdio;

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

const READER_PATH_ENV: &str = "FSQLITE_GH411_FOREIGN_READER_DB";
const READER_READY: &str = "foreign-reader-ready";
const READER_ROLLED_BACK: &str = "foreign-reader-rolled-back";
const TEST_NAME: &str = "commit_survives_foreign_canonical_reader_pinning_a_snapshot";

fn scalar_i64(rows: &[Row]) -> i64 {
    match rows[0].values()[0] {
        SqliteValue::Integer(n) => n,
        ref other => panic!("expected an integer, got {other:?}"),
    }
}

fn sidecar(db: &Path, suffix: &str) -> std::path::PathBuf {
    let mut path = db.as_os_str().to_owned();
    path.push(suffix);
    std::path::PathBuf::from(path)
}

fn wal_len(db: &Path) -> u64 {
    std::fs::metadata(sidecar(db, "-wal")).map_or(0, |meta| meta.len())
}

/// `(mxFrame, nBackfill, aReadMark[0..5])` straight out of the `-shm`
/// WAL-index header, so a probe can tell what the shared index publishes.
///
/// Layout (C SQLite): `[0..48)` header copy 1, `[48..96)` copy 2,
/// `[96..100)` `nBackfill`, `[100..120)` `aReadMark`. `mxFrame` sits at
/// offset 16 of a copy. All native-endian.
fn shm_header(db: &Path) -> (u32, u32, Vec<u32>) {
    let Ok(bytes) = std::fs::read(sidecar(db, "-shm")) else {
        return (0, 0, Vec::new());
    };
    if bytes.len() < 120 {
        return (0, 0, Vec::new());
    }
    let word = |off: usize| {
        u32::from_ne_bytes([bytes[off], bytes[off + 1], bytes[off + 2], bytes[off + 3]])
    };
    (
        word(16),
        word(96),
        (0..5).map(|i| word(100 + i * 4)).collect(),
    )
}

fn canonical_count(db: &Path) -> i64 {
    let conn = rusqlite::Connection::open(db).expect("stock handle opens the database");
    conn.query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
        .expect("stock handle counts rows")
}

fn expect_line(expected: &str) {
    let mut line = String::new();
    std::io::stdin()
        .lock()
        .read_line(&mut line)
        .expect("child reads a control line");
    assert_eq!(line.trim(), expected, "child control protocol");
}

/// The child half: pin a stock read snapshot, announce readiness, roll back on
/// `release`, and only exit on `exit`. Splitting rollback from process exit is
/// what proves the destroyer is the CLOSE path and not the rollback.
fn run_as_foreign_reader(db_path: &std::ffi::OsStr) {
    let reader = rusqlite::Connection::open(Path::new(db_path)).expect("child opens stock reader");
    reader
        .execute_batch("BEGIN;")
        .expect("child begins a read transaction");
    let pinned: i64 = reader
        .query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
        .expect("child pins its read snapshot");
    println!("{READER_READY} {pinned}");
    std::io::stdout().flush().expect("flush readiness witness");

    expect_line("release");
    reader
        .execute_batch("ROLLBACK;")
        .expect("child releases its read transaction");
    println!("{READER_ROLLED_BACK}");
    std::io::stdout().flush().expect("flush rollback witness");

    expect_line("exit");
}

fn signal_child(child: &mut std::process::Child, line: &str) {
    writeln!(
        child.stdin.as_mut().expect("child stdin stays available"),
        "{line}"
    )
    .expect("signal the foreign reader");
}

/// Own only this keeper's child and stdout reader through every assertion.
/// A timeout or failed oracle kills/reaps that exact child and joins the pipe
/// reader, so a stuck native admission cannot strand the integration target.
struct PublicReaderProcess {
    child: std::process::Child,
    output: std::sync::mpsc::Receiver<String>,
    reader_thread: Option<std::thread::JoinHandle<()>>,
    kind: &'static str,
}

impl PublicReaderProcess {
    fn spawn(path: &Path, kind: &'static str) -> Self {
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "gh19_public_connections_publish_pinned_and_fresh_stock_views",
                "--exact",
                "--nocapture",
            ])
            .env("FSQLITE_GH19_PUBLIC_READER_DB", path.as_os_str())
            .env("FSQLITE_GH19_PUBLIC_READER_KIND", kind)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn exact public-reader child");
        let (sender, output) = std::sync::mpsc::channel();
        let mut owned = Self { child, output, reader_thread: None, kind };
        let stdout = owned.child.stdout.take().expect("child stdout pipe");
        owned.reader_thread = Some(std::thread::spawn(move || {
            let mut output = std::io::BufReader::new(stdout);
            loop {
                let mut line = String::new();
                match output.read_line(&mut line) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        if sender.send(line).is_err() {
                            break;
                        }
                    }
                }
            }
        }));
        owned
    }

    fn signal(&mut self, line: &str) {
        signal_child(&mut self.child, line);
    }

    fn witness(&self, expected: &str) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let now = std::time::Instant::now();
            assert!(now < deadline,
                "{} reader failed to produce {expected} within 30 seconds", self.kind);
            let remaining = deadline.saturating_duration_since(now);
            let line = self.output.recv_timeout(remaining).unwrap_or_else(|error| {
                panic!("{} reader failed to produce {expected} within 30 seconds: {error}", self.kind)
            });
            if line.trim_end().ends_with(expected) {
                return;
            }
        }
    }

    fn wait_success(&mut self) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let status = loop {
            if let Some(status) = self.child.try_wait().expect("poll exact reader child") {
                break status;
            }
            assert!(std::time::Instant::now() < deadline,
                "{} reader failed to exit within 30 seconds", self.kind);
            std::thread::sleep(std::time::Duration::from_millis(10));
        };
        if let Some(reader) = self.reader_thread.take() {
            reader.join().expect("join completed stdout reader");
        }
        assert!(status.success(), "{} reader exited with {status}", self.kind);
    }
}

impl Drop for PublicReaderProcess {
    fn drop(&mut self) {
        // Closing stdin lets a healthy child leave its control loop; kill is
        // bounded to the exact process spawned above if it has not exited.
        drop(self.child.stdin.take());
        if !matches!(self.child.try_wait(), Ok(Some(_))) {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
        if let Some(reader) = self.reader_thread.take() {
            let _ = reader.join();
        }
    }
}

/// Public native and stock readers keep their old snapshots, then observe new
/// commits through the same attached handles, across a checkpoint generation.
#[test]
fn gh19_public_connections_publish_pinned_and_fresh_stock_views() {
    const CHILD_DB: &str = "FSQLITE_GH19_PUBLIC_READER_DB";
    const CHILD_KIND: &str = "FSQLITE_GH19_PUBLIC_READER_KIND";
    if let Some(path) = std::env::var_os(CHILD_DB) {
        #[cfg(all(unix, feature = "native"))]
        match std::env::var(CHILD_KIND).unwrap().as_str() {
            "segment-writer" => {
                run_as_segment_writer(Path::new(&path));
                return;
            }
            "segment-stock" => {
                run_as_segment_stock_reader(Path::new(&path));
                return;
            }
            "segment-native" => {
                run_as_segment_native_reader(Path::new(&path));
                return;
            }
            _ => {}
        }
        #[cfg(all(unix, feature = "native"))]
        if std::env::var(CHILD_KIND).unwrap() == "export" {
            run_as_public_export(Path::new(&path));
            return;
        }
        if std::env::var(CHILD_KIND).unwrap() == "fresh-stock" {
            // Keep stock's classic POSIX lock lifecycle out of the native
            // writer process while its attachment and SHM claims remain live.
            assert_eq!(canonical_count(Path::new(&path)), 2,
                "a fresh stock reader sees native publication");
            println!("gh19-fresh-stock");
            std::io::stdout().flush().unwrap();
            return;
        }
        if std::env::var(CHILD_KIND).unwrap() == "checkpoint" {
            asupersync::test_utils::run_test(|| async {
                let checkpoint = Connection::open(Path::new(&path).to_str().unwrap()).await.unwrap();
                println!("gh19-checkpoint-ready");
                std::io::stdout().flush().unwrap();
                for (command, witness, busy, oracle) in [
                    ("pinned-old", "gh19-checkpoint-old", 1, "pinned foreign readers defer reset"),
                    ("pinned-latest", "gh19-checkpoint-latest", 1, "current-generation readers still own their reset gates"),
                    ("native-only", "gh19-checkpoint-native-only", 1, "the public native reader alone must defer foreign reset"),
                    ("idle", "gh19-checkpoint-idle", 0, "idle attachments permit a complete checkpoint"),
                ] {
                    expect_line(command);
                    let rows = checkpoint.query("PRAGMA wal_checkpoint(TRUNCATE)").await.unwrap();
                    assert_eq!(scalar_i64(&rows), busy, "{oracle}");
                    println!("{witness}");
                    std::io::stdout().flush().unwrap();
                }
                expect_line("exit");
                checkpoint.close().await.unwrap();
            });
            return;
        }
        if std::env::var(CHILD_KIND).unwrap() == "stock" {
            let reader = rusqlite::Connection::open(Path::new(&path)).unwrap();
            reader.execute_batch("BEGIN").unwrap();
            let count = || reader.query_row("SELECT COUNT(*) FROM t", [], |row| row.get::<_, i64>(0)).unwrap();
            assert_eq!(count(), 1);
            println!("gh19-ready");
            std::io::stdout().flush().unwrap();
            expect_line("pinned");
            assert_eq!(count(), 1, "stock snapshot remains pinned after the new commit");
            println!("gh19-pinned");
            std::io::stdout().flush().unwrap();
            expect_line("renew");
            reader.execute_batch("ROLLBACK; BEGIN").unwrap();
            assert_eq!(count(), 2, "same stock attachment sees the published commit");
            println!("gh19-renewed");
            std::io::stdout().flush().unwrap();
            expect_line("idle");
            reader.execute_batch("ROLLBACK").unwrap();
            println!("gh19-idle");
            std::io::stdout().flush().unwrap();
            expect_line("after-reset");
            reader.execute_batch("BEGIN").unwrap();
            assert_eq!(count(), 3, "stock attachment follows the reset generation");
            reader.execute_batch("COMMIT").unwrap();
            let integrity: String = reader.query_row("PRAGMA integrity_check", [], |row| row.get(0)).unwrap();
            assert_eq!(integrity, "ok");
            println!("gh19-after-reset");
            std::io::stdout().flush().unwrap();
            expect_line("exit");
        } else {
            asupersync::test_utils::run_test(|| async {
                let reader = Connection::open(Path::new(&path).to_str().unwrap()).await.unwrap();
                reader.execute("BEGIN").await.unwrap();
                assert_eq!(scalar_i64(&reader.query("SELECT COUNT(*) FROM t").await.unwrap()), 1);
                println!("gh19-ready");
                std::io::stdout().flush().unwrap();
                expect_line("pinned");
                assert_eq!(scalar_i64(&reader.query("SELECT COUNT(*) FROM t").await.unwrap()), 1,
                    "native snapshot remains pinned after the new commit");
                println!("gh19-pinned");
                std::io::stdout().flush().unwrap();
                expect_line("renew");
                reader.execute("ROLLBACK").await.unwrap();
                reader.execute("BEGIN").await.unwrap();
                assert_eq!(scalar_i64(&reader.query("SELECT COUNT(*) FROM t").await.unwrap()), 2,
                    "same native attachment sees the published commit");
                println!("gh19-renewed");
                std::io::stdout().flush().unwrap();
                expect_line("idle");
                reader.execute("ROLLBACK").await.unwrap();
                println!("gh19-idle");
                std::io::stdout().flush().unwrap();
                expect_line("after-reset");
                reader.execute("BEGIN").await.unwrap();
                assert_eq!(scalar_i64(&reader.query("SELECT COUNT(*) FROM t").await.unwrap()), 3,
                    "native attachment follows the reset generation");
                reader.execute("COMMIT").await.unwrap();
                println!("gh19-after-reset");
                std::io::stdout().flush().unwrap();
                expect_line("exit");
                reader.close().await.unwrap();
            });
        }
        return;
    }

    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gh19-public-readers.db");
        let writer = Connection::open(path.to_str().unwrap()).await.unwrap();
        writer.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, k TEXT)").await.unwrap();
        writer.execute("INSERT INTO t VALUES(1, 'baseline')").await.unwrap();
        let mut checkpoint = PublicReaderProcess::spawn(&path, "checkpoint");
        checkpoint.witness("gh19-checkpoint-ready");
        let mut readers = Vec::new();
        for kind in ["stock", "native"] {
            let child = PublicReaderProcess::spawn(&path, kind);
            child.witness("gh19-ready");
            readers.push(child);
        }
        writer.execute("INSERT INTO t VALUES(2, 'while-pinned')").await.unwrap();
        for child in &mut readers {
            child.signal("pinned");
            child.witness("gh19-pinned");
        }
        let mut fresh = PublicReaderProcess::spawn(&path, "fresh-stock");
        fresh.witness("gh19-fresh-stock");
        fresh.wait_success();
        let wal_before = std::fs::read(sidecar(&path, "-wal")).unwrap();
        checkpoint.signal("pinned-old");
        checkpoint.witness("gh19-checkpoint-old");
        assert_eq!(std::fs::read(sidecar(&path, "-wal")).unwrap(), wal_before,
            "deferred reset preserves WAL bytes");
        for child in &mut readers {
            child.signal("renew");
            child.witness("gh19-renewed");
        }
        checkpoint.signal("pinned-latest");
        checkpoint.witness("gh19-checkpoint-latest");
        assert_eq!(std::fs::read(sidecar(&path, "-wal")).unwrap(), wal_before,
            "latest pinned readers preserve the current generation");
        let stock = readers.iter_mut().find(|reader| reader.kind == "stock").unwrap();
        stock.signal("idle");
        stock.witness("gh19-idle");
        // A stock reader must not mask a missing public native reader claim.
        // Keep the native transaction pinned while the stock attachment is idle.
        checkpoint.signal("native-only");
        checkpoint.witness("gh19-checkpoint-native-only");
        assert_eq!(std::fs::read(sidecar(&path, "-wal")).unwrap(), wal_before,
            "the native reader alone preserves the current WAL generation");
        let native = readers.iter_mut().find(|reader| reader.kind == "native").unwrap();
        native.signal("idle");
        native.witness("gh19-idle");
        checkpoint.signal("idle");
        checkpoint.witness("gh19-checkpoint-idle");
        assert_eq!(wal_len(&path), 32, "completed checkpoint retains only the new WAL header");
        writer.execute("INSERT INTO t VALUES(3, 'new-generation')").await.unwrap();
        for child in &mut readers {
            child.signal("after-reset");
            child.witness("gh19-after-reset");
        }
        for child in &mut readers {
            child.signal("exit");
            child.wait_success();
        }
        checkpoint.signal("exit");
        checkpoint.wait_success();
        writer.close().await.unwrap();
        assert_eq!(canonical_count(&path), 3, "all commits survive every participant closing");
        let stock = rusqlite::Connection::open(&path).unwrap();
        let integrity: String = stock.query_row("PRAGMA integrity_check", [], |row| row.get(0)).unwrap();
        assert_eq!(integrity, "ok");
    });
}

/// The exact public-reader child filter also hosts this export control. The
/// parent bounds each witness and owns kill/reap through any failed assertion.
#[cfg(all(unix, feature = "native"))]
fn run_as_public_export(path: &Path) {
    asupersync::test_utils::run_test(|| async {
        let writer = Connection::open(path.to_str().unwrap()).await.unwrap();
        writer.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, k TEXT)").await.unwrap();
        writer.execute("INSERT INTO t VALUES(1, 'first'), (2, 'second')").await.unwrap();
        assert_eq!(scalar_i64(&writer.query("SELECT COUNT(*) FROM t").await.unwrap()), 2);
        println!("gh19-export-ready");
        std::io::stdout().flush().unwrap();
        expect_line("export");
        let bytes = writer.export_bytes().await.expect("public native export completes");
        assert!(bytes.starts_with(b"SQLite format 3\0"));
        std::fs::write(sidecar(path, "-export.db"), &bytes).unwrap();
        println!("gh19-exported");
        std::io::stdout().flush().unwrap();
        expect_line("continue");
        writer.execute("INSERT INTO t VALUES(3, 'after-export')").await.unwrap();
        assert_eq!(scalar_i64(&writer.query("SELECT COUNT(*) FROM t").await.unwrap()), 3);
        writer.close().await.expect("writer closes after export and further commit");
        println!("gh19-export-continued");
        std::io::stdout().flush().unwrap();
    });
}

/// Public export must release its native copy window and preserve a standalone
/// stock-readable snapshot while the same original connection keeps writing.
#[cfg(all(unix, feature = "native"))]
#[test]
fn gh19_public_export_bytes_preserves_committed_rows_and_writer_progress() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("gh19-public-export.db");
    let mut child = PublicReaderProcess::spawn(&path, "export");
    child.witness("gh19-export-ready");
    assert!(wal_len(&path) > 32, "committed rows have a physical WAL source");
    child.signal("export");
    // The old db-file read-guard/native-map write-guard cycle times out here.
    // PublicReaderProcess then kills/reaps only this exact child on unwind.
    child.witness("gh19-exported");
    let exported_path = sidecar(&path, "-export.db");
    assert!(!sidecar(&exported_path, "-wal").exists(), "export is a standalone main image");
    // This is a different inode: never open stock on the original database
    // while its native writer and POSIX ownership ledger remain live.
    let exported = rusqlite::Connection::open(&exported_path).unwrap();
    let rows: String = exported.query_row(
        "SELECT group_concat(id || ':' || k, ',') FROM (SELECT id, k FROM t ORDER BY id)",
        [], |row| row.get(0),
    ).unwrap();
    assert_eq!(rows, "1:first,2:second");
    let integrity: String = exported
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap();
    assert_eq!(integrity, "ok");
    child.signal("continue");
    child.witness("gh19-export-continued");
    child.wait_success();
    let export_count: i64 = exported
        .query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
        .unwrap();
    assert_eq!(export_count, 2, "later original commits do not change the exported snapshot");
    assert_eq!(
        canonical_count(&path), 3,
        "same original writer progressed and committed after export"
    );
    let original = rusqlite::Connection::open(&path).unwrap();
    let integrity: String = original
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap();
    assert_eq!(integrity, "ok");
}

#[cfg(all(unix, feature = "native"))]
fn segment_payload(id: i64) -> Vec<u8> {
    let (length, salt) = match id {
        0 => (128, 0x19_u8),
        1 => (2_200_000, 0x3D_u8),
        2 => (2_200_000, 0xB7_u8),
        _ => panic!("unexpected segment fixture row {id}"),
    };
    (0..length)
        .map(|index| {
            u8::try_from(index % 251)
                .unwrap()
                .wrapping_mul(17)
                .wrapping_add(salt)
        })
        .collect()
}

#[cfg(all(unix, feature = "native"))]
fn assert_segment_stock_rows(connection: &rusqlite::Connection, last_id: i64) {
    let mut statement = connection.prepare("SELECT id, payload FROM t ORDER BY id").unwrap();
    let rows = statement
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(rows.len(), usize::try_from(last_id + 1).unwrap());
    for (expected_id, (id, payload)) in (0..=last_id).zip(rows) {
        assert_eq!(id, expected_id);
        assert!(payload == segment_payload(id), "exact stock payload for row {id}");
    }
}

#[cfg(all(unix, feature = "native"))]
async fn assert_segment_native_rows(connection: &Connection, last_id: i64) {
    let rows = connection.query("SELECT id, payload FROM t ORDER BY id").await.unwrap();
    assert_eq!(rows.len(), usize::try_from(last_id + 1).unwrap());
    for (id, row) in (0..=last_id).zip(rows) {
        assert_eq!(row.values().len(), 2);
        assert_eq!(row.values()[0], SqliteValue::Integer(id));
        let SqliteValue::Blob(payload) = &row.values()[1] else {
            panic!("native row {id} must contain a BLOB");
        };
        assert!(
            payload.as_ref() == segment_payload(id).as_slice(),
            "exact native payload for row {id}",
        );
    }
}

#[cfg(all(unix, feature = "native"))]
fn run_as_segment_writer(path: &Path) {
    asupersync::test_utils::run_test(|| async {
        let writer = Connection::open(path.to_str().unwrap()).await.unwrap();
        assert!(writer.is_concurrent_mode_default());
        assert_eq!(scalar_i64(&writer.query("PRAGMA page_size").await.unwrap()), 512);
        assert_eq!(
            writer.query_row("PRAGMA journal_mode=WAL").await.unwrap().values(),
            &[SqliteValue::Text("wal".into())],
        );
        writer.execute("PRAGMA synchronous=FULL").await.unwrap();
        writer.execute("PRAGMA wal_autocheckpoint=0").await.unwrap();
        assert_eq!(
            writer.query_row("PRAGMA synchronous").await.unwrap().values(),
            &[SqliteValue::Integer(2)],
        );
        writer.execute_with_params(
            "INSERT INTO t VALUES(?1, ?2)",
            &[SqliteValue::Integer(0), SqliteValue::Blob(segment_payload(0).into())],
        ).await.unwrap();
        println!("segments-writer-ready");
        std::io::stdout().flush().unwrap();
        for (id, command, witness) in [
            (1, "first", "segments-written-first"),
            (2, "second", "segments-written-second"),
        ] {
            expect_line(command);
            writer.execute_with_params(
                "INSERT INTO t VALUES(?1, ?2)",
                &[SqliteValue::Integer(id), SqliteValue::Blob(segment_payload(id).into())],
            ).await.expect("FULL native commit crosses the next SHM segment");
            assert!(writer.is_concurrent_mode_default());
            println!("{witness}");
            std::io::stdout().flush().unwrap();
        }
        expect_line("exit");
        writer.close().await.unwrap();
    });
}

#[cfg(all(unix, feature = "native"))]
fn run_as_segment_stock_reader(path: &Path) {
    let reader = rusqlite::Connection::open(path).unwrap();
    reader.execute_batch("BEGIN").unwrap();
    assert_segment_stock_rows(&reader, 0);
    println!("segments-reader-ready");
    std::io::stdout().flush().unwrap();
    for (id, phase) in [(1, "first"), (2, "second")] {
        expect_line(&format!("{phase}-pinned"));
        assert_segment_stock_rows(&reader, id - 1);
        println!("segments-pinned-{phase}");
        std::io::stdout().flush().unwrap();
        expect_line(&format!("{phase}-renew"));
        reader.execute_batch("ROLLBACK; BEGIN").unwrap();
        assert_segment_stock_rows(&reader, id);
        println!("segments-renewed-{phase}");
        std::io::stdout().flush().unwrap();
    }
    expect_line("exit");
    reader.execute_batch("ROLLBACK").unwrap();
    let integrity: String = reader.query_row("PRAGMA integrity_check", [], |row| row.get(0)).unwrap();
    assert_eq!(integrity, "ok");
}

#[cfg(all(unix, feature = "native"))]
fn run_as_segment_native_reader(path: &Path) {
    asupersync::test_utils::run_test(|| async {
        let reader = Connection::open(path.to_str().unwrap()).await.unwrap();
        reader.execute("BEGIN").await.unwrap();
        assert_segment_native_rows(&reader, 0).await;
        println!("segments-reader-ready");
        std::io::stdout().flush().unwrap();
        for (id, phase) in [(1, "first"), (2, "second")] {
            expect_line(&format!("{phase}-pinned"));
            assert_segment_native_rows(&reader, id - 1).await;
            println!("segments-pinned-{phase}");
            std::io::stdout().flush().unwrap();
            expect_line(&format!("{phase}-renew"));
            reader.execute("ROLLBACK").await.unwrap();
            reader.execute("BEGIN").await.unwrap();
            assert_segment_native_rows(&reader, id).await;
            println!("segments-renewed-{phase}");
            std::io::stdout().flush().unwrap();
        }
        expect_line("exit");
        reader.execute("ROLLBACK").await.unwrap();
        reader.close().await.unwrap();
    });
}

#[cfg(all(unix, feature = "native"))]
struct NativeSegmentPublication {
    header: fsqlite_wal::wal_index::WalIndexHdr,
    shared: Vec<u8>,
    wal: Vec<u8>,
}

/// Only the diagnostic parent calls this: it holds no original-inode SQL or
/// VFS handle. All children are waiting at explicit witnesses, so these are
/// stable bytes published by the native writer before any stock refresh.
#[cfg(all(unix, feature = "native"))]
fn read_native_segment_publication(path: &Path) -> NativeSegmentPublication {
    use fsqlite_wal::wal_index::{
        WAL_SHM_FIRST_HEADER_BYTES, WAL_SHM_SEGMENT_BYTES, WalIndexFrameLocation,
        lookup_native_wal_index_frame, parse_shm_header,
    };
    use fsqlite_wal::{WalFrameHeader, WalHeader, validate_wal_header_checksum};

    let wal = std::fs::read(sidecar(path, "-wal")).unwrap();
    let shared = std::fs::read(sidecar(path, "-shm")).unwrap();
    let wal_header = WalHeader::from_bytes(&wal).unwrap();
    assert_eq!(wal_header.page_size, 512);
    assert!(validate_wal_header_checksum(&wal, wal_header.big_endian_checksum()).unwrap());
    let frame_size = 24 + usize::try_from(wal_header.page_size).unwrap();
    assert_eq!((wal.len() - 32) % frame_size, 0, "no partial physical WAL frame");
    let frame_count = u32::try_from((wal.len() - 32) / frame_size).unwrap();
    assert!(frame_count > 0);
    let (header, checkpoint) = parse_shm_header(&shared)
        .unwrap()
        .expect("native writer published matching initialized dual headers");
    assert_eq!(header.mx_frame, frame_count, "publication precedes stock recovery");
    assert_eq!(header.page_size().unwrap(), 512);
    assert_eq!(header.a_salt, [wal_header.salts.salt1, wal_header.salts.salt2]);
    assert_eq!(header.big_end_cksum, u8::from(wal_header.big_endian_checksum()));
    assert_eq!(checkpoint.n_backfill, 0, "autocheckpoint remains disabled");
    let last = WalIndexFrameLocation::new(frame_count).unwrap();
    let region_count = usize::try_from(last.region).unwrap() + 1;
    assert!(shared.len() >= region_count * WAL_SHM_SEGMENT_BYTES);
    for frame_no in 1..=frame_count {
        let offset = 32 + usize::try_from(frame_no - 1).unwrap() * frame_size;
        let frame = WalFrameHeader::from_bytes(&wal[offset..]).unwrap();
        assert_eq!(frame.salts, wal_header.salts);
        let location = WalIndexFrameLocation::new(frame_no).unwrap();
        let region_start = usize::try_from(location.region).unwrap() * WAL_SHM_SEGMENT_BYTES;
        let segment = &shared[region_start..region_start + WAL_SHM_SEGMENT_BYTES];
        let header_bytes = if location.region == 0 { WAL_SHM_FIRST_HEADER_BYTES } else { 0 };
        let page_offset = header_bytes + (usize::from(location.entry) - 1) * 4;
        assert_eq!(
            u32::from_ne_bytes(segment[page_offset..page_offset + 4].try_into().unwrap()),
            frame.page_number,
            "native page-array entry for physical frame {frame_no}",
        );
        assert_eq!(
            lookup_native_wal_index_frame(segment, location.region, frame.page_number, frame_no)
                .unwrap(),
            Some(frame_no),
            "native hash entry for physical frame {frame_no}",
        );
        if frame_no == frame_count {
            assert!(frame.is_commit());
            assert_eq!(header.n_page, frame.db_size);
            assert_eq!(header.a_frame_cksum, [frame.checksum.s1, frame.checksum.s2]);
        }
    }
    eprintln!(
        "GH19 live native FULL publication: frames={frame_count} regions={region_count} shm_bytes={}",
        shared.len(),
    );
    NativeSegmentPublication { header, shared, wal }
}

#[cfg(all(unix, feature = "native"))]
fn assert_native_segment_prefix(prior: &NativeSegmentPublication, next: &NativeSegmentPublication) {
    use fsqlite_wal::wal_index::{
        WAL_SHM_FIRST_HEADER_BYTES, WAL_SHM_PAGE_ARRAY_BYTES, WAL_SHM_SEGMENT_BYTES,
    };

    assert_eq!(next.header.a_salt, prior.header.a_salt, "no reset between native commits");
    assert_eq!(next.header.i_change, prior.header.i_change.wrapping_add(1));
    assert_eq!(&next.wal[..prior.wal.len()], prior.wal, "retain the committed WAL prefix");
    for (index, segment) in prior.shared.as_chunks::<WAL_SHM_SEGMENT_BYTES>().0.iter().enumerate() {
        let next_segment =
            &next.shared[index * WAL_SHM_SEGMENT_BYTES..(index + 1) * WAL_SHM_SEGMENT_BYTES];
        let header_bytes = if index == 0 { WAL_SHM_FIRST_HEADER_BYTES } else { 0 };
        for offset in (header_bytes..WAL_SHM_PAGE_ARRAY_BYTES).step_by(4) {
            if segment[offset..offset + 4] != [0; 4] {
                assert_eq!(&next_segment[offset..offset + 4], &segment[offset..offset + 4]);
            }
        }
        for offset in (WAL_SHM_PAGE_ARRAY_BYTES..WAL_SHM_SEGMENT_BYTES).step_by(2) {
            if segment[offset..offset + 2] != [0; 2] {
                assert_eq!(&next_segment[offset..offset + 2], &segment[offset..offset + 2]);
            }
        }
    }
}

/// Exercise real native mmap growth at both hash-segment boundaries, with
/// FULL commits and independent readers retaining exact old payloads. This
/// exercises the native producer path; the stock-only codec fixture stays separate.
#[cfg(all(unix, feature = "native"))]
#[test]
fn gh19_public_native_full_commits_grow_shared_segments_before_stock_refresh() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("native-segment-growth.db");
    {
        let seed = rusqlite::Connection::open(&path).unwrap();
        seed.execute_batch(
            "PRAGMA page_size=512; CREATE TABLE t(id INTEGER PRIMARY KEY, payload BLOB NOT NULL)",
        ).unwrap();
    }
    // Every original-inode connection lives in a child from this point on.
    // The parent can inspect SHM without closing a descriptor behind a live
    // same-process native/stock POSIX lock owner.
    let mut writer = PublicReaderProcess::spawn(&path, "segment-writer");
    writer.witness("segments-writer-ready");
    let mut prior = read_native_segment_publication(&path);
    assert!(prior.header.mx_frame <= 4062);
    assert_eq!(prior.shared.len(), 32_768, "baseline maps only region zero");
    let mut readers = [
        PublicReaderProcess::spawn(&path, "segment-stock"),
        PublicReaderProcess::spawn(&path, "segment-native"),
    ];
    for reader in &readers {
        reader.witness("segments-reader-ready");
    }
    for (phase, lower, upper) in [("first", 4062, 8158), ("second", 8158, u32::MAX)] {
        writer.signal(phase);
        writer.witness(&format!("segments-written-{phase}"));
        // No reader is released to query/renew before these physical oracles.
        let published = read_native_segment_publication(&path);
        assert!(published.header.mx_frame > lower && published.header.mx_frame <= upper);
        assert!(published.shared.len() > prior.shared.len(), "native commit grows another mmap region");
        assert_native_segment_prefix(&prior, &published);
        for reader in &mut readers {
            reader.signal(&format!("{phase}-pinned"));
            reader.witness(&format!("segments-pinned-{phase}"));
        }
        for reader in &mut readers {
            reader.signal(&format!("{phase}-renew"));
            reader.witness(&format!("segments-renewed-{phase}"));
        }
        prior = published;
    }
    for reader in &mut readers {
        reader.signal("exit");
        reader.wait_success();
    }
    writer.signal("exit");
    writer.wait_success();
    let reopened = rusqlite::Connection::open(&path).unwrap();
    assert_segment_stock_rows(&reopened, 2);
    let integrity: String = reopened.query_row("PRAGMA integrity_check", [], |row| row.get(0)).unwrap();
    assert_eq!(integrity, "ok");
}

/// Stock owns separate seed inodes throughout the copied-file constructor tests.
/// Capture its nonzero reader mark before any native target handle is opened.
#[cfg(all(unix, feature = "native"))]
struct PublicBootstrapSeed {
    _stock: rusqlite::Connection,
    main: Vec<u8>,
    wal: Vec<u8>,
    shared: Vec<u8>,
}

#[cfg(all(unix, feature = "native"))]
fn public_bootstrap_seed(directory: &Path) -> PublicBootstrapSeed {
    let path = directory.join("bootstrap-seed.db");
    let stock = rusqlite::Connection::open(&path).unwrap();
    stock
        .execute_batch(
            "PRAGMA page_size=4096; PRAGMA journal_mode=WAL; \
             PRAGMA wal_autocheckpoint=0; CREATE TABLE t(n INTEGER); \
             INSERT INTO t VALUES(10),(20); BEGIN;",
        )
        .unwrap();
    let sum: i64 = stock
        .query_row("SELECT sum(n) FROM t", [], |row| row.get(0))
        .unwrap();
    assert_eq!(sum, 30);
    let main = std::fs::read(&path).unwrap();
    let wal = std::fs::read(sidecar(&path, "-wal")).unwrap();
    let shared = std::fs::read(sidecar(&path, "-shm")).unwrap();
    let (header, checkpoint) = fsqlite_wal::wal_index::parse_shm_header(&shared)
        .unwrap()
        .expect("stock publishes a complete fixture header");
    assert!(header.mx_frame > 0);
    assert!(checkpoint.n_backfill < header.mx_frame);
    assert!(checkpoint.a_read_mark[1..].contains(&header.mx_frame));
    PublicBootstrapSeed {
        _stock: stock,
        main,
        wal,
        shared,
    }
}

/// Every target is a fresh copy; stock never opens these target inodes while
/// a native constructor or connection owns their attachment and reader gates.
#[cfg(all(unix, feature = "native"))]
fn copy_public_bootstrap_seed(
    seed: &PublicBootstrapSeed,
    path: &Path,
    kind: &str,
) -> Option<Vec<u8>> {
    use fsqlite_wal::wal_index::{WAL_INDEX_HDR_BYTES, WalIndexHdr};

    assert!(!path.exists());
    std::fs::write(path, &seed.main).unwrap();
    std::fs::write(sidecar(path, "-wal"), &seed.wal).unwrap();
    if kind == "missing" {
        assert!(!sidecar(path, "-shm").exists());
        return None;
    }
    let mut shared = seed.shared.clone();
    match kind {
        "valid" => {}
        "invalid" => shared[0] ^= 1,
        "stale" => {
            let mut header = WalIndexHdr::from_bytes(&shared).unwrap();
            header.a_salt[0] ^= 1;
            header.update_checksum().unwrap();
            for start in [0, WAL_INDEX_HDR_BYTES] {
                shared[start..start + WAL_INDEX_HDR_BYTES]
                    .copy_from_slice(&header.to_bytes());
            }
        }
        _ => panic!("unknown bootstrap fixture {kind}"),
    }
    std::fs::write(sidecar(path, "-shm"), &shared).unwrap();
    Some(shared)
}

/// Inspect only after the target Connection has completed shutdown. Opening
/// and closing a diagnostic main/SHM descriptor earlier drops Linux F_SETLK
/// claims behind the native VFS's retained ownership ledger.
#[cfg(all(unix, feature = "native"))]
fn assert_closed_public_bootstrap_wal_binding(path: &Path) {
    use fsqlite_wal::wal_index::{parse_shm_header, validate_shared_wal_index_wal_binding};
    use fsqlite_wal::{WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, WalFrameHeader, WalHeader};

    let shared = std::fs::read(sidecar(path, "-shm")).unwrap();
    let (header, _) = parse_shm_header(&shared)
        .unwrap()
        .expect("public constructor and commit publish a coherent shared header");
    let wal = std::fs::read(sidecar(path, "-wal")).unwrap();
    let wal_header = WalHeader::from_bytes(&wal).unwrap();
    let frame_size = WAL_FRAME_HEADER_SIZE + usize::try_from(wal_header.page_size).unwrap();
    assert!(wal.len() > WAL_HEADER_SIZE);
    assert_eq!((wal.len() - WAL_HEADER_SIZE) % frame_size, 0);
    let frame_count = (wal.len() - WAL_HEADER_SIZE) / frame_size;
    assert_eq!(usize::try_from(header.mx_frame).unwrap(), frame_count);
    let offset = WAL_HEADER_SIZE + (frame_count - 1) * frame_size;
    let terminal = WalFrameHeader::from_bytes(&wal[offset..]).unwrap();
    validate_shared_wal_index_wal_binding(
        &header,
        &wal_header,
        Some((header.mx_frame, terminal)),
    )
    .expect("shared publication names the actual committed WAL terminal frame");
}

#[cfg(all(unix, feature = "native"))]
#[test]
fn gh19_public_fresh_open_bootstraps_native_wal_before_schema_and_writes() {
    asupersync::test_utils::run_test(|| async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("public-fresh-bootstrap.db");
        assert!(!path.exists());
        let connection = Connection::open(path.to_str().unwrap())
            .await
            .expect("fresh public open transfers its bootstrap admission");
        connection.execute("CREATE TABLE t(n INTEGER)").await.unwrap();
        connection.execute("INSERT INTO t VALUES(10),(20)").await.unwrap();
        assert_eq!(
            scalar_i64(&connection.query("SELECT sum(n) FROM t").await.unwrap()),
            30
        );
        connection.close_without_checkpoint().await.unwrap();
        assert_closed_public_bootstrap_wal_binding(&path);
        assert_eq!(canonical_count(&path), 2, "stock sees the public fresh-file commit");
    });
}

#[cfg(all(unix, feature = "native"))]
#[test]
fn gh19_public_existing_writable_open_recovers_missing_invalid_and_stale_shm() {
    asupersync::test_utils::run_test(|| async {
        let directory = tempfile::tempdir().unwrap();
        let seed = public_bootstrap_seed(directory.path());
        for schema_only in [false, true] {
            for kind in ["missing", "invalid", "stale"] {
                let path = directory.path().join(format!("writable-{schema_only}-{kind}.db"));
                let _ = copy_public_bootstrap_seed(&seed, &path, kind);
                let connection = if schema_only {
                    Connection::open_existing_schema_only(path.to_str().unwrap()).await
                } else {
                    Connection::open_existing(path.to_str().unwrap()).await
                }
                .unwrap_or_else(|error| {
                    panic!("public writable constructor schema_only={schema_only} kind={kind}: {error}")
                });
                assert_eq!(
                    scalar_i64(&connection.query("SELECT sum(n) FROM t").await.unwrap()),
                    30,
                    "the complete stock WAL remains authoritative at public admission"
                );
                connection.execute("INSERT INTO t VALUES(30)").await.unwrap();
                assert_eq!(
                    scalar_i64(&connection.query("SELECT sum(n) FROM t").await.unwrap()),
                    60
                );
                connection.close_without_checkpoint().await.unwrap();
                // Full existing-open may perform its normal first-open migration;
                // validate the final committed generation, not pre-migration bytes.
                assert_closed_public_bootstrap_wal_binding(&path);
                assert_eq!(canonical_count(&path), 3, "stock reopens the repaired public commit");
            }
        }
    });
}

#[cfg(all(unix, feature = "native"))]
#[test]
fn gh19_public_readonly_schema_open_requires_valid_shm_without_storage_mutation() {
    asupersync::test_utils::run_test(|| async {
        let directory = tempfile::tempdir().unwrap();
        let seed = public_bootstrap_seed(directory.path());
        for kind in ["valid", "missing", "invalid", "stale"] {
            let path = directory.path().join(format!("readonly-{kind}.db"));
            let shared_before = copy_public_bootstrap_seed(&seed, &path, kind);
            let result = Connection::open_schema_only(path.to_str().unwrap()).await;
            if kind == "valid" {
                let connection = result.expect("read-only public open uses an existing stock reader mark");
                assert_eq!(
                    scalar_i64(&connection.query("SELECT sum(n) FROM t").await.unwrap()),
                    30
                );
                let error = connection.execute("INSERT INTO t VALUES(30)").await.unwrap_err();
                assert!(matches!(error, fsqlite_error::FrankenError::ReadOnly), "{error}");
                connection.close_without_checkpoint().await.unwrap();
            } else {
                let error = match result {
                    Ok(connection) => {
                        connection.close_without_checkpoint().await.unwrap();
                        panic!("read-only public open must refuse {kind} native index");
                    }
                    Err(error) => error,
                };
                assert!(
                    matches!(error, fsqlite_error::FrankenError::BusyRecovery),
                    "read-only {kind} refuses required recovery explicitly: {error}"
                );
            }
            // Constructor refusal or explicit close has completed before these
            // descriptor opens; no live target lock ledger is invalidated.
            assert_eq!(std::fs::read(&path).unwrap(), seed.main, "{kind}: main image preserved");
            assert_eq!(std::fs::read(sidecar(&path, "-wal")).unwrap(), seed.wal,
                "{kind}: committed WAL preserved");
            if let Some(shared_before) = shared_before {
                assert_eq!(std::fs::read(sidecar(&path, "-shm")).unwrap(), shared_before,
                    "{kind}: readonly admission never repairs or changes shared metadata");
            } else {
                assert!(!sidecar(&path, "-shm").exists(), "readonly refusal never creates SHM");
            }
        }
    });
}

/// Preserve the attachment-lifetime boundary when
/// the writer closes before the pinned stock reader releases its snapshot.
/// Keep both close orders in the default regression suite.
#[test]
fn committed_row_survives_writer_close_before_foreign_reader_exit() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("gh411-writer-closes-first.db");
        let conn = Connection::open(db_path.to_str().unwrap())
            .await
            .expect("open writer");
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, k TEXT UNIQUE);")
            .await
            .expect("create table");
        conn.execute("INSERT INTO t(k) VALUES ('baseline');")
            .await
            .expect("baseline row");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([TEST_NAME, "--exact", "--nocapture"])
            .env(READER_PATH_ENV, db_path.as_os_str())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn pinned stock reader");
        let mut child_out = std::io::BufReader::new(child.stdout.take().unwrap());
        loop {
            let mut line = String::new();
            assert!(child_out.read_line(&mut line).unwrap() > 0);
            if line.contains(READER_READY) {
                break;
            }
        }
        let wal_before_commit = wal_len(&db_path);
        conn.execute("INSERT INTO t(k) VALUES ('committed-before-writer-close');")
            .await
            .expect("commit while stock reader is pinned");
        assert_eq!(
            scalar_i64(&conn.query("SELECT COUNT(*) FROM t;").await.unwrap()),
            2
        );
        let committed_wal_len = wal_len(&db_path);
        assert!(
            committed_wal_len > wal_before_commit,
            "the acknowledged commit must append a WAL tail"
        );
        eprintln!(
            "gh411 reverse-close before writer close: wal={committed_wal_len} shm={:?}",
            shm_header(&db_path)
        );
        conn.close().await.expect("writer closes before reader");
        eprintln!(
            "gh411 reverse-close writer closed, reader pinned: wal={} shm={:?}",
            wal_len(&db_path),
            shm_header(&db_path)
        );

        signal_child(&mut child, "release");
        let mut rolled = String::new();
        child_out.read_line(&mut rolled).unwrap();
        assert!(rolled.contains(READER_ROLLED_BACK));
        eprintln!(
            "gh411 reverse-close reader rolled back, process alive: wal={} shm={:?}",
            wal_len(&db_path),
            shm_header(&db_path)
        );
        signal_child(&mut child, "exit");
        assert!(child.wait().unwrap().success());
        let surviving_rows = canonical_count(&db_path);
        eprintln!(
            "gh411 reverse-close reader exited: wal={} stock_rows={surviving_rows}",
            wal_len(&db_path)
        );
        assert_eq!(
            surviving_rows, 2,
            "acknowledged rows must survive both close orders (committed WAL {committed_wal_len})"
        );
    });
}

#[test]
fn commit_survives_foreign_canonical_reader_pinning_a_snapshot() {
    if let Some(db_path) = std::env::var_os(READER_PATH_ENV) {
        run_as_foreign_reader(&db_path);
        return;
    }

    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("gh411-foreign-reader.db");
    let db_str = db_path.to_string_lossy().into_owned();

    // One long-lived fsqlite connection throughout, the way a pooled writer
    // lives: seed, write while the foreign reader is pinned, checkpoint, close.
    asupersync::test_utils::run_test(move || {
        let db_path = db_path.clone();
        let db_str = db_str.clone();
        async move {
            let conn = Connection::open(&db_str).await.expect("open writer");
            conn.execute("PRAGMA journal_mode=WAL;")
                .await
                .expect("wal mode");
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, k TEXT UNIQUE);")
                .await
                .expect("create table");
            conn.execute("INSERT INTO t(k) VALUES('baseline');")
                .await
                .expect("baseline insert");

            // ── A foreign canonical reader pins a snapshot. ───────────────
            let mut child = std::process::Command::new(
                std::env::current_exe().expect("resolve the test executable"),
            )
            .arg(TEST_NAME)
            .arg("--exact")
            .arg("--nocapture")
            .env(READER_PATH_ENV, db_path.as_os_str())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn the foreign canonical reader");
            let mut child_out =
                std::io::BufReader::new(child.stdout.take().expect("capture child stdout"));
            let mut ready = false;
            loop {
                let mut line = String::new();
                if child_out.read_line(&mut line).expect("read child stdout") == 0 {
                    break;
                }
                if line.contains(READER_READY) {
                    ready = true;
                    break;
                }
            }
            assert!(ready, "the foreign reader exited before pinning a snapshot");

            // ── fsqlite commits while that snapshot is pinned. ────────────
            let wal_before = wal_len(&db_path);
            conn.execute("INSERT INTO t(k) VALUES('while-reader-pinned');")
                .await
                .expect("insert commits while a foreign reader is active");
            assert_eq!(
                scalar_i64(&conn.query("SELECT COUNT(*) FROM t;").await.expect("count")),
                2,
                "the writer sees its own committed row"
            );
            let wal_after_commit = wal_len(&db_path);
            assert!(
                wal_after_commit > wal_before,
                "the commit appended frames to the WAL ({wal_before} -> {wal_after_commit})"
            );
            // Safe to open a throwaway stock handle here: the child still
            // pins its read transaction, so this handle's own close cannot
            // take the EXCLUSIVE lock that would delete the WAL.
            eprintln!(
                "gh411 after-commit, reader pinned: wal={wal_after_commit} stock={} shm={:?}",
                canonical_count(&db_path),
                shm_header(&db_path)
            );

            // A passive checkpoint may legitimately publish nothing — the
            // foreign reader pins the tail — but must never discard frames.
            conn.execute("PRAGMA wal_checkpoint(PASSIVE);")
                .await
                .expect("passive checkpoint");
            eprintln!(
                "gh411 after-passive-checkpoint: wal={} shm={:?}",
                wal_len(&db_path),
                shm_header(&db_path)
            );

            // ── Release the read transaction, child process still alive. ──
            signal_child(&mut child, "release");
            let mut rolled = String::new();
            child_out
                .read_line(&mut rolled)
                .expect("read the rollback witness");
            assert!(
                rolled.contains(READER_ROLLED_BACK),
                "the foreign reader rolled back, got {rolled:?}"
            );
            eprintln!(
                "gh411 after-rollback, child alive: wal={} shm={:?} fsqlite={}",
                wal_len(&db_path),
                shm_header(&db_path),
                scalar_i64(&conn.query("SELECT COUNT(*) FROM t;").await.expect("count"))
            );

            // ── Now let the child PROCESS exit: this is the destroyer. ────
            signal_child(&mut child, "exit");
            assert!(
                child.wait().expect("wait for the foreign reader").success(),
                "the foreign reader child failed"
            );
            let wal_after_close = wal_len(&db_path);
            eprintln!(
                "gh411 after the child process exits: wal={wal_after_close} shm={:?}",
                shm_header(&db_path)
            );

            assert_eq!(
                scalar_i64(&conn.query("SELECT COUNT(*) FROM t;").await.expect("count")),
                2,
                "a foreign connection's close must not discard committed frames \
                 (wal: {wal_before} -> {wal_after_commit} -> {wal_after_close})"
            );
            conn.close().await.expect("close writer");

            assert_eq!(
                canonical_count(&db_path),
                2,
                "the committed row must be visible to a fresh stock-SQLite handle"
            );

            let conn = Connection::open(&db_str).await.expect("reopen");
            assert_eq!(
                scalar_i64(&conn.query("SELECT COUNT(*) FROM t;").await.expect("count")),
                2,
                "the committed row survives close and reopen"
            );
            conn.close().await.expect("close");
        }
    });
}

/// Validate the codec against headers actually emitted by bundled stock
/// SQLite. This is format evidence; it does not exercise FrankenSQLite's
/// still-pending native shared-index publication path.
#[test]
fn gh19_wal_index_codec_accepts_stock_headers_and_commit_counters() {
    use fsqlite_wal::wal_index::{WAL_INDEX_HDR_BYTES, WalIndexHdr, parse_shm_header};
    use fsqlite_wal::{WalFrameHeader, WalHeader};

    for page_size in [512_u32, 4096, 65_536] {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("stock-wal-index.db");
        let stock = rusqlite::Connection::open(&db).expect("stock open");
        stock
            .execute_batch(&format!(
                "PRAGMA page_size={page_size}; PRAGMA journal_mode=WAL; \
                 PRAGMA wal_autocheckpoint=0; CREATE TABLE t(id INTEGER PRIMARY KEY); \
                 INSERT INTO t VALUES(1);"
            ))
            .expect("stock fixture");
        let before_bytes = std::fs::read(sidecar(&db, "-shm")).expect("stock SHM");
        let (before, _) = parse_shm_header(&before_bytes)
            .expect("parse stock SHM")
            .expect("stock publishes valid matching headers");
        let schema_before: u32 = stock
            .query_row("PRAGMA schema_version", [], |row| row.get(0))
            .expect("schema cookie");

        stock.execute("INSERT INTO t VALUES(2)", []).expect("commit");
        let bytes = std::fs::read(sidecar(&db, "-shm")).expect("new stock SHM");
        let (header, _) = parse_shm_header(&bytes)
            .expect("parse stock SHM")
            .expect("published stock header");
        assert_eq!(header.i_change, before.i_change.wrapping_add(1));
        assert!(header.mx_frame > before.mx_frame);
        let schema_after: u32 = stock
            .query_row("PRAGMA schema_version", [], |row| row.get(0))
            .expect("schema cookie after data-only commit");
        assert_eq!(schema_before, schema_after, "iChange is not the schema cookie");
        assert_eq!(header.page_size().expect("page-size decode"), page_size);
        if page_size == 65_536 {
            assert_eq!(header.sz_page, 1);
        }

        let wal = std::fs::read(sidecar(&db, "-wal")).expect("stock WAL");
        let wal_header = WalHeader::from_bytes(&wal).expect("WAL header");
        assert_eq!(
            header.a_salt,
            [wal_header.salts.salt1, wal_header.salts.salt2]
        );
        assert_eq!(&bytes[32..40], &wal[16..24], "salt bytes are copied verbatim");
        assert_eq!(header.big_end_cksum, u8::from(wal_header.big_endian_checksum()));
        let frame_offset = 32
            + usize::try_from(header.mx_frame - 1).expect("frame index")
                * (24 + usize::try_from(page_size).expect("page size"));
        let frame = WalFrameHeader::from_bytes(&wal[frame_offset..]).expect("commit frame");
        assert_eq!(header.n_page, frame.db_size);
        assert_eq!(header.a_frame_cksum, [frame.checksum.s1, frame.checksum.s2]);
        let mut checksummed = header;
        checksummed.a_cksum = [0; 2];
        checksummed.update_checksum().expect("native checksum");
        assert_eq!(checksummed.a_cksum, header.a_cksum, "stock checksum oracle");
        assert_eq!(header.to_bytes(), bytes[..WAL_INDEX_HDR_BYTES]);

        let mut damaged = bytes;
        damaged[16] ^= 1;
        damaged[WAL_INDEX_HDR_BYTES + 16] ^= 1;
        assert!(parse_shm_header(&damaged).expect("parse damaged").is_none());
        assert!(
            WalIndexHdr::from_bytes(&damaged)
                .expect("decode damaged")
                .validate()
                .is_err()
        );
        eprintln!(
            "GH19 stock codec: page_size={page_size} mxFrame={} iChange={}->{}",
            header.mx_frame, before.i_change, header.i_change
        );
    }
}

#[test]
fn gh19_native_hash_bytes_match_stock_across_segment_boundaries() {
    use fsqlite_wal::WalFrameHeader;
    use fsqlite_wal::wal_index::{
        WAL_SHM_FIRST_HEADER_BYTES, WAL_SHM_SEGMENT_BYTES, WalIndexFrameLocation,
        append_native_wal_index_entry, lookup_native_wal_index_frame, parse_shm_header,
    };

    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("stock-wal-hash.db");
    let stock = rusqlite::Connection::open(&db).expect("stock open");
    // This checks live byte layout, not crash durability. Avoid 9000 fsyncs
    // in the stock fixture; the FrankenSQLite durability keepers are separate.
    stock
        .execute_batch(
            "PRAGMA page_size=4096; PRAGMA journal_mode=WAL; \
             PRAGMA wal_autocheckpoint=0; PRAGMA synchronous=OFF; \
             CREATE TABLE t(n INTEGER); INSERT INTO t VALUES(0);",
        )
        .expect("stock fixture");
    let mut update = stock.prepare("UPDATE t SET n=n+1").expect("prepare");
    for _ in 0..9000 {
        assert_eq!(update.execute([]).expect("stock append"), 1);
    }
    drop(update);
    let bytes = std::fs::read(sidecar(&db, "-shm")).expect("stock SHM");
    let (header, _) = parse_shm_header(&bytes)
        .expect("parse")
        .expect("published stock header");
    assert!(header.mx_frame > 8159, "exercise first and subsequent boundaries");
    let last = WalIndexFrameLocation::new(header.mx_frame).expect("last frame");
    let region_count = usize::try_from(last.region).expect("region") + 1;
    let mut rebuilt = vec![vec![0; WAL_SHM_SEGMENT_BYTES]; region_count];
    rebuilt[0][..WAL_SHM_FIRST_HEADER_BYTES]
        .copy_from_slice(&bytes[..WAL_SHM_FIRST_HEADER_BYTES]);
    let wal = std::fs::read(sidecar(&db, "-wal")).expect("stock WAL");
    let mut pages = Vec::new();
    for frame_no in 1..=header.mx_frame {
        let offset = 32 + usize::try_from(frame_no - 1).expect("frame") * (24 + 4096);
        let frame = WalFrameHeader::from_bytes(&wal[offset..]).expect("frame header");
        let location = WalIndexFrameLocation::new(frame_no).expect("frame location");
        let region = usize::try_from(location.region).expect("region");
        append_native_wal_index_entry(&mut rebuilt[region], frame_no, frame.page_number)
            .expect("reconstruct from actual WAL frames");
        pages.push(frame.page_number);
    }
    for (region, reconstructed) in rebuilt.iter().enumerate() {
        let offset = region * WAL_SHM_SEGMENT_BYTES;
        assert_eq!(
            reconstructed.as_slice(),
            &bytes[offset..offset + WAL_SHM_SEGMENT_BYTES],
            "native region {region} must match stock byte for byte"
        );
    }
    for horizon in [0, 1, 4061, 4062, 4063, 8158, 8159, header.mx_frame] {
        for page in [1, 2, 8199] {
            let expected = pages[..usize::try_from(horizon).expect("horizon")]
                .iter()
                .rposition(|&candidate| candidate == page)
                .map(|index| u32::try_from(index + 1).expect("frame number"));
            let actual = rebuilt.iter().enumerate().rev().find_map(|(region, _)| {
                let offset = region * WAL_SHM_SEGMENT_BYTES;
                lookup_native_wal_index_frame(
                    &bytes[offset..offset + WAL_SHM_SEGMENT_BYTES],
                    u32::try_from(region).expect("region"),
                    page,
                    horizon,
                )
                .expect("lookup in stock bytes")
            });
            assert_eq!(actual, expected, "page={page} horizon={horizon}");
        }
    }
    eprintln!(
        "GH19 native hash oracle: mxFrame={} regions={region_count} boundary=4062/4096",
        header.mx_frame
    );
}

/// The public commit path must own the same physical WRITE gate as a stock
/// process. This does not establish shared-index publication after commit.
#[test]
fn gh19_physical_append_refuses_foreign_stock_writer_and_retries() {
    const CHILD_PATH: &str = "FSQLITE_GH19_STOCK_WRITER_DB";
    const TEST: &str = "gh19_physical_append_refuses_foreign_stock_writer_and_retries";
    if let Some(path) = std::env::var_os(CHILD_PATH) {
        let writer = rusqlite::Connection::open(Path::new(&path)).expect("stock writer");
        writer.execute_batch("PRAGMA busy_timeout=0; BEGIN IMMEDIATE; UPDATE t SET k='uncommitted';")
            .expect("stock owns WRITE with an uncommitted update");
        println!("stock-writer-owns-write");
        std::io::stdout().flush().unwrap();
        expect_line("release");
        writer.execute_batch("ROLLBACK;").expect("release stock WRITE");
        println!("stock-writer-released");
        std::io::stdout().flush().unwrap();
        expect_line("exit");
        return;
    }
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("gh19-stock-writer.db");
        let conn = Connection::open(db.to_str().unwrap()).await.expect("native writer");
        conn.execute("PRAGMA wal_autocheckpoint=0;").await.unwrap();
        conn.execute("PRAGMA busy_timeout=1;").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, k TEXT);").await.unwrap();
        conn.execute("INSERT INTO t VALUES(1,'baseline');").await.unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([TEST, "--exact", "--nocapture"])
            .env(CHILD_PATH, &db)
            .stdin(Stdio::piped()).stdout(Stdio::piped()).spawn().unwrap();
        let mut output = std::io::BufReader::new(child.stdout.take().unwrap());
        let mut wait_for = |expected: &str| {
            loop {
                let mut line = String::new();
                assert_ne!(output.read_line(&mut line).unwrap(), 0, "child exited before {expected}");
                if line.trim() == expected {
                    break;
                }
            }
        };
        wait_for("stock-writer-owns-write");
        // The stock peer has initialized the shared index before we pin a
        // native snapshot. Concurrent preparation must remain admitted even
        // though that peer already owns the physical WRITE gate.
        conn.execute("BEGIN;").await.unwrap();
        conn.execute("INSERT INTO t VALUES(2,'native');").await.unwrap();
        let before = std::fs::read(sidecar(&db, "-wal")).expect("WAL before refused append");
        let error = conn.execute("COMMIT;").await.expect_err("stock WRITE must exclude native append");
        assert!(matches!(error, fsqlite_error::FrankenError::Busy), "exact physical contention: {error:?}");
        assert_eq!(std::fs::read(sidecar(&db, "-wal")).unwrap(), before,
            "refused physical append must not change WAL bytes");
        conn.execute("ROLLBACK;").await.expect("rollback refused transaction");
        signal_child(&mut child, "release");
        wait_for("stock-writer-released");
        conn.execute("BEGIN;").await.unwrap();
        conn.execute("INSERT INTO t VALUES(2,'native');").await.unwrap();
        conn.execute("COMMIT;").await.expect("retry after stock releases WRITE");
        assert_eq!(scalar_i64(&conn.query("SELECT COUNT(*) FROM t;").await.unwrap()), 2);
        signal_child(&mut child, "exit");
        assert!(child.wait().unwrap().success());
        conn.close().await.expect("close native writer");
        assert_eq!(canonical_count(&db), 2, "stock reopen sees the acknowledged retry");
    });
}

/// Physical WAL initialization must not replace a sidecar while a foreign
/// reader owns the main image. This exercises the pager's actual Unix fence;
/// the connection unit keeper separately covers the final sidecar recheck.
#[cfg(unix)]
#[test]
fn gh19_wal_initialization_refuses_foreign_reader_without_mutation() {
    const CHILD_PATH: &str = "FSQLITE_GH19_INITIALIZATION_READER_DB";
    const TEST: &str = "gh19_wal_initialization_refuses_foreign_reader_without_mutation";
    if let Some(path) = std::env::var_os(CHILD_PATH) {
        run_as_foreign_reader(&path);
        return;
    }
    asupersync::test_utils::run_test(|| async {
        use fsqlite_types::cx::Cx;
        use fsqlite_types::flags::VfsOpenFlags;
        use fsqlite_vfs::Vfs as _;

        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("gh19-wal-initialization.db");
        {
            let stock = rusqlite::Connection::open(&db).unwrap();
            stock.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY); INSERT INTO t VALUES(1);")
                .unwrap();
        }
        let wal_path = sidecar(&db, "-wal");
        let invalid = vec![0x71_u8; 64];
        std::fs::write(&wal_path, &invalid).unwrap();
        let cx = Cx::new();
        let vfs = fsqlite_vfs::UnixVfs::new();
        let pager = fsqlite_pager::SimplePager::open_with_cx(
            &cx,
            vfs,
            &db,
            fsqlite_types::PageSize::DEFAULT,
        )
        .await
        .unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([TEST, "--exact", "--nocapture"])
            .env(CHILD_PATH, &db)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut output = std::io::BufReader::new(child.stdout.take().unwrap());
        loop {
            let mut line = String::new();
            assert_ne!(output.read_line(&mut line).unwrap(), 0);
            if line.starts_with(READER_READY) {
                break;
            }
        }
        let mut entered = false;
        let error = pager
            .with_wal_initialization(&cx, &mut entered, |_, entered| {
                Box::pin(async move {
                    *entered = true;
                    Ok(())
                })
            })
            .await
            .expect_err("foreign main-file reader must exclude WAL initialization");
        assert!(matches!(error, fsqlite_error::FrankenError::Busy), "{error:?}");
        assert!(!entered);
        assert_eq!(std::fs::read(&wal_path).unwrap(), invalid);
        signal_child(&mut child, "release");
        signal_child(&mut child, "exit");
        assert!(child.wait().unwrap().success());

        let mut state = (fsqlite_vfs::UnixVfs::new(), wal_path);
        let wal = pager
            .with_wal_initialization(&cx, &mut state, |cx, (vfs, path)| {
                Box::pin(async move {
                    let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
                    let (file, _) = vfs.open(cx, Some(path), flags)?;
                    fsqlite_wal::WalFile::create(
                        cx,
                        file,
                        fsqlite_types::PageSize::DEFAULT.get(),
                        0,
                        fsqlite_wal::WalSalts::generate(),
                    )
                    .await
                })
            })
            .await
            .expect("retry after foreign reader release must create the WAL");
        assert_eq!(wal.frame_count(), 0);
        wal.close(&cx).unwrap();
    });
}
