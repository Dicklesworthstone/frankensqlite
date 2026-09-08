//! GH#416: filesystems that number a file by its first data cluster (macOS
//! `msdosfs` / FAT32) give a zero-length file a placeholder `st_ino` that
//! changes on the first write. A database CREATED on such a volume captured
//! that placeholder as its namespace identity, so the first page write made
//! every later path probe classify the same file as superseded
//! (`unable to open database file`).
//!
//! Two tests:
//!
//! * `database_created_on_a_placeholder_inode_mount_survives_its_first_write`
//!   is the real-mount check, opt-in through `FSQLITE_PERMISSIONLESS_FS_DIR`
//!   (the same variable the namespace crate uses for permission-less mounts:
//!   point it at a writable directory on a FAT volume, e.g.
//!   `hdiutil create -size 64m -fs "MS-DOS FAT32" -volname FSQFAT x.dmg &&
//!   hdiutil attach x.dmg` then `/Volumes/FSQFAT`). It creates a database
//!   there, writes through the placeholder->cluster transition, joins a
//!   second in-process connection after the transition, and reopens.
//! * `corrupt_certificate_suffix_fails_closed_without_wedging_the_writer`
//!   runs everywhere: garbage after the newest `-wal-cert` record must fail
//!   every write closed with the record-boundary diagnostic, must never hang
//!   the connection that hit it or a peer, and must stop failing once the
//!   sidecar is repaired.
//!
//! Every step runs under a watchdog that aborts the whole process on overrun:
//! a hang IS the defect under test, so it has to surface as a fast failure.

use std::path::Path;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::Duration;

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const STEP_TIMEOUT: Duration = Duration::from_secs(120);

struct Watchdog {
    done: mpsc::Sender<()>,
}

fn watchdog(label: &'static str) -> Watchdog {
    let (done, finished) = mpsc::channel::<()>();
    std::thread::spawn(move || {
        if finished.recv_timeout(STEP_TIMEOUT) == Err(RecvTimeoutError::Timeout) {
            eprintln!("GH#416 watchdog: step `{label}` exceeded {STEP_TIMEOUT:?}; aborting");
            std::process::exit(101);
        }
    });
    Watchdog { done }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        let _ = self.done.send(());
    }
}

async fn open(path: &Path) -> Connection {
    let conn = Connection::open(path.to_str().expect("utf-8 path"))
        .await
        .expect("open connection");
    conn.query("PRAGMA journal_mode = WAL;")
        .await
        .expect("switch to WAL");
    conn
}

async fn count_rows(conn: &Connection) -> i64 {
    let rows = conn
        .query("SELECT COUNT(*) FROM t;")
        .await
        .expect("count rows");
    match rows[0].values()[0] {
        SqliteValue::Integer(n) => n,
        ref other => panic!("expected integer count, got {other:?}"),
    }
}

#[test]
fn database_created_on_a_placeholder_inode_mount_survives_its_first_write() {
    let Some(root) = std::env::var_os("FSQLITE_PERMISSIONLESS_FS_DIR") else {
        eprintln!("FSQLITE_PERMISSIONLESS_FS_DIR unset; skipping");
        return;
    };
    let dir = tempfile::Builder::new()
        .prefix("fsqlite-gh416-")
        .tempdir_in(root)
        .expect("tempdir on the mount");
    let db = dir.path().join("created.db");

    asupersync::test_utils::run_test(|| async {
        let creator = {
            let _guard = watchdog("create the database on the mount");
            Connection::open(db.to_str().expect("utf-8 path"))
                .await
                .expect("open a fresh database on the mount")
        };
        {
            let _guard = watchdog("first write (page 1 materializes the file)");
            creator
                .execute("CREATE TABLE t(x INTEGER);")
                .await
                .expect("the first write must survive the placeholder->cluster inode transition");
        }
        {
            let _guard = watchdog("writes after the transition");
            creator
                .execute("INSERT INTO t VALUES (1), (2), (3);")
                .await
                .expect("second write");
            assert_eq!(count_rows(&creator).await, 3);
        }
        {
            // A connection opened AFTER the transition derives the
            // materialized identity from its own descriptor; it must join
            // the creator's generation (namespace record + in-process
            // registries), not fail admission or fork a parallel one.
            let _guard = watchdog("peer connection joins after the transition");
            let peer = Connection::open(db.to_str().expect("utf-8 path"))
                .await
                .expect("peer open after the transition");
            peer.execute("INSERT INTO t VALUES (4);")
                .await
                .expect("peer write");
            assert_eq!(count_rows(&creator).await, 4);
            assert_eq!(count_rows(&peer).await, 4);
            peer.close().await.expect("close peer");
        }
        creator.close().await.expect("close creator");
        {
            let _guard = watchdog("reopen after everything closed");
            let reopened = Connection::open(db.to_str().expect("utf-8 path"))
                .await
                .expect("reopen");
            assert_eq!(count_rows(&reopened).await, 4);
            reopened
                .execute("INSERT INTO t VALUES (5);")
                .await
                .expect("write after reopen");
            reopened.close().await.expect("close reopened");
        }
    });
}

#[test]
fn corrupt_certificate_suffix_fails_closed_without_wedging_the_writer() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("cert.db");
    let cert = dir.path().join("cert.db-wal-cert");

    asupersync::test_utils::run_test(|| async {
        let conn = open(&db).await;
        conn.execute("CREATE TABLE t(x INTEGER);")
            .await
            .expect("create table");
        conn.execute("INSERT INTO t VALUES (1);")
            .await
            .expect("first insert");
        let clean_len = std::fs::metadata(&cert)
            .expect("certificate sidecar exists after a WAL commit")
            .len();
        assert!(clean_len > 0);

        // Non-record bytes after the newest record: what a torn/foreign tail
        // looks like to `validate_incomplete_certificate_suffix`.
        {
            use std::io::Write as _;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&cert)
                .expect("append to sidecar");
            file.write_all(b"GARBAGEGARBAGEGARBAGE")
                .expect("append garbage");
        }

        {
            let _guard = watchdog("first write after the sidecar tail was corrupted");
            let error = conn
                .execute("INSERT INTO t VALUES (2);")
                .await
                .expect_err("a corrupt certificate tail must fail the write closed");
            let message = error.to_string();
            assert!(
                message.contains("record boundary"),
                "unexpected diagnostic: {message}"
            );
        }
        {
            // The connection that hit the error must not be wedged: the next
            // attempt returns (any result) instead of hanging on a lock it
            // still holds.
            let _guard = watchdog("second write on the same connection");
            let _ = conn.execute("INSERT INTO t VALUES (3);").await;
        }
        {
            // Nor may it wedge a peer: RESERVED must have been released.
            let _guard = watchdog("peer connection while the failed writer is alive");
            match Connection::open(db.to_str().expect("utf-8 path")).await {
                Ok(peer) => {
                    let _ = peer.execute("INSERT INTO t VALUES (3);").await;
                    let _ = peer.close_without_checkpoint().await;
                }
                Err(error) => eprintln!("peer open refused (fail-closed is fine): {error}"),
            }
        }

        // Repair the sidecar: everything past the last complete record goes.
        {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&cert)
                .expect("reopen sidecar");
            file.set_len(clean_len).expect("truncate garbage");
        }
        {
            let _guard = watchdog("peer write after the sidecar was repaired");
            let peer = Connection::open(db.to_str().expect("utf-8 path"))
                .await
                .expect("open after repair");
            peer.execute("INSERT INTO t VALUES (4);")
                .await
                .expect("writes resume once the sidecar is repaired");
            assert!(count_rows(&peer).await >= 2);
            peer.close_without_checkpoint().await.expect("close peer");
        }
        {
            let _guard = watchdog("original connection after the repair");
            let _ = conn.execute("INSERT INTO t VALUES (5);").await;
            let _ = conn.close_without_checkpoint().await;
        }
    });
}
