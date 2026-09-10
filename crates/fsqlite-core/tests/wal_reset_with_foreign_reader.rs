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

/// Explicit investigation of the remaining attachment-lifetime boundary when
/// the writer closes before the pinned stock reader releases its snapshot.
/// This must pass before GH411 can claim reverse-close-order durability;
/// it is separate from the unchanged original regression below.
#[test]
#[ignore = "GH411 reverse-close-order investigation; run explicitly before issue closure"]
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
