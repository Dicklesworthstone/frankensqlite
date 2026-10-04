#![recursion_limit = "512"]

//! bd-sz9j5: a read-write open must refuse to join a WAL database that a
//! pre-0.4 fsqlite engine holds open in another process, and must keep
//! joining every 0.4.x-style peer.
//!
//! A 0.3.x connection never maps the stock `-shm` outside its tests and takes
//! main-file locks only inside transactions, so while idle it holds nothing but
//! a shared `flock` on `<db>-fsqlite-ns-use` (observed with the v0.3.9 and
//! v0.3.18 release CLIs). Every 0.4.x WAL connection also holds the `-shm` DMS
//! byte and, since 0.4.4, a read lock on the main file's shared range. The
//! legacy peer here reproduces the 0.3.x signature exactly. Peers run in child
//! processes (this test binary re-executed) because `F_GETLK` cannot see the
//! calling process's own locks.

#[cfg(all(unix, feature = "native"))]
mod unix_only {
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command};
    use std::time::{Duration, Instant};

    use fsqlite_core::connection::Connection;
    use fsqlite_error::{FrankenError, SQLITE_OPEN_INCOMPATIBLE_PEER};
    use fsqlite_types::value::SqliteValue;

    const ROLE: &str = "BD_SZ9J5_CHILD_ROLE";
    const DB: &str = "BD_SZ9J5_DB";
    const READY: &str = "BD_SZ9J5_READY";
    const RELEASE: &str = "BD_SZ9J5_RELEASE";
    const CHILD_TEST: &str = "unix_only::bd_sz9j5_child_peer";

    fn sidecar(db: &Path, suffix: &str) -> PathBuf {
        let mut name = db.as_os_str().to_owned();
        name.push(suffix);
        PathBuf::from(name)
    }

    fn wait_for(path: &Path, what: &str) {
        let deadline = Instant::now() + Duration::from_secs(60);
        while !path.exists() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// A peer process that holds `role`'s open state on `db` until released.
    struct Peer {
        child: Child,
        release: PathBuf,
    }

    impl Peer {
        fn spawn(role: &str, db: &Path) -> Self {
            let dir = db.parent().unwrap();
            let ready = dir.join(format!("{role}.ready"));
            let release = dir.join(format!("{role}.release"));
            let _ = std::fs::remove_file(&ready);
            let _ = std::fs::remove_file(&release);
            let child = Command::new(std::env::current_exe().unwrap())
                .args([CHILD_TEST, "--exact", "--nocapture", "--test-threads=1"])
                .env(ROLE, role)
                .env(DB, db)
                .env(READY, &ready)
                .env(RELEASE, &release)
                .spawn()
                .unwrap();
            wait_for(&ready, role);
            Self { child, release }
        }

        fn finish(mut self) {
            std::fs::write(&self.release, b"go").unwrap();
            let status = self.child.wait().unwrap();
            assert!(status.success(), "peer process failed: {status:?}");
        }
    }

    fn file_state(db: &Path) -> Vec<(String, Option<Vec<u8>>)> {
        ["", "-wal", "-shm", "-fsqlite-ns-use"]
            .into_iter()
            .map(|suffix| {
                (
                    suffix.to_owned(),
                    std::fs::read(sidecar(db, suffix)).ok(),
                )
            })
            .collect()
    }

    fn seed(db: &Path) {
        // Seed from a child so this process never registers the inode.
        Peer::spawn("seed", db).finish();
        let header = std::fs::read(db).unwrap();
        assert_eq!(&header[18..20], &[2, 2], "seed must leave a WAL-mode file");
        assert!(sidecar(db, "-fsqlite-ns-use").exists());
    }

    fn count_rows(db: &Path) -> i64 {
        let stock = rusqlite::Connection::open(db).unwrap();
        let check: String = stock
            .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
            .unwrap();
        assert_eq!(check, "ok");
        stock
            .query_row("SELECT count(*) FROM t;", [], |row| row.get(0))
            .unwrap()
    }

    /// Child entry point; a no-op unless re-executed by a test below.
    #[test]
    fn bd_sz9j5_child_peer() {
        let Ok(role) = std::env::var(ROLE) else {
            return;
        };
        let db = PathBuf::from(std::env::var(DB).unwrap());
        let ready = PathBuf::from(std::env::var(READY).unwrap());
        let release = PathBuf::from(std::env::var(RELEASE).unwrap());
        match role.as_str() {
            "legacy" => {
                // The idle 0.3.x signature: a shared flock on the namespace
                // `use` sidecar and nothing else.
                let use_file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(sidecar(&db, "-fsqlite-ns-use"))
                    .unwrap();
                use_file.lock_shared().unwrap();
                std::fs::write(&ready, b"ready").unwrap();
                wait_for(&release, "release");
                drop(use_file);
            }
            "pager_joiner" => {
                // A 0.4.x joiner stopped between namespace `bind` (which
                // releases the admission gate) and its WAL attach: a pager
                // opened without the connection bootstrap that attaches the
                // WAL. It must not show the 0.3.x lock signature.
                asupersync::test_utils::run_test(|| async {
                    let cx = fsqlite_types::cx::Cx::new();
                    let pager = fsqlite_pager::SimplePager::open_with_cx(
                        &cx,
                        fsqlite_vfs::UnixVfs::new(),
                        &db,
                        fsqlite_types::PageSize::DEFAULT,
                    )
                    .await
                    .unwrap();
                    std::fs::write(&ready, b"ready").unwrap();
                    wait_for(&release, "release");
                    drop(pager);
                });
            }
            "seed" | "fsqlite_rw" | "fsqlite_ro" => {
                asupersync::test_utils::run_test(|| async {
                    let path = db.to_str().unwrap().to_owned();
                    let conn = if role == "fsqlite_ro" {
                        Connection::open_schema_only(path).await.unwrap()
                    } else {
                        Connection::open(path).await.unwrap()
                    };
                    if role == "seed" {
                        conn.execute("PRAGMA journal_mode=WAL;").await.unwrap();
                        conn.execute("CREATE TABLE t(x);").await.unwrap();
                        conn.execute("INSERT INTO t VALUES (1);").await.unwrap();
                    } else {
                        conn.query_row("SELECT count(*) FROM t;").await.unwrap();
                    }
                    std::fs::write(&ready, b"ready").unwrap();
                    wait_for(&release, "release");
                    conn.close().await.unwrap();
                });
            }
            other => panic!("unknown peer role {other}"),
        }
    }

    #[test]
    fn read_write_open_beside_legacy_peer_is_refused_without_writes() {
        if std::env::var(ROLE).is_ok() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("legacy-peer.db");
        seed(&db);

        let legacy = Peer::spawn("legacy", &db);
        let before = file_state(&db);
        let started = Instant::now();
        let mut error = None;
        asupersync::test_utils::run_test(|| async {
            match Connection::open(db.to_str().unwrap()).await {
                Ok(conn) => {
                    conn.execute("INSERT INTO t VALUES (2);").await.unwrap();
                    conn.close().await.unwrap();
                }
                Err(open_error) => error = Some(open_error),
            }
        });
        let elapsed = started.elapsed();
        let error = error.expect("a read-write open beside a 0.3.x-style peer must be refused");
        assert!(
            matches!(&error, FrankenError::IncompatiblePeerEngine { path } if path.ends_with("legacy-peer.db")),
            "unexpected error: {error:?}"
        );
        assert_eq!(error.extended_error_code(), SQLITE_OPEN_INCOMPATIBLE_PEER);
        assert!(
            elapsed < Duration::from_secs(10),
            "refusal must be prompt, took {elapsed:?}"
        );
        assert_eq!(file_state(&db), before, "a refused open must write nothing");
        legacy.finish();

        // Once the legacy peer is gone the same open proceeds.
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(db.to_str().unwrap()).await.unwrap();
            conn.execute("INSERT INTO t VALUES (2);").await.unwrap();
            conn.close().await.unwrap();
        });
        assert_eq!(count_rows(&db), 2);
    }

    /// A joiner that has bound its namespace admission (releasing the gate)
    /// but not yet attached the WAL is the only other holder once the
    /// established peer exits. A stalled joiner (CPU starvation, slow storage)
    /// can stay there longer than the retry budget, so it must already carry
    /// a 0.4.x lock; otherwise a third opener falsely refuses with
    /// `IncompatiblePeerEngine` although every process is 0.4.x.
    #[test]
    fn read_write_open_beside_a_joiner_before_wal_attach_is_admitted() {
        if std::env::var(ROLE).is_ok() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("joiner-window.db");
        seed(&db);
        let established = Peer::spawn("fsqlite_rw", &db);
        let joiner = Peer::spawn("pager_joiner", &db);
        established.finish();
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(db.to_str().unwrap())
                .await
                .unwrap_or_else(|error| {
                    panic!("open beside a 0.4.x joiner before its WAL attach must be admitted: {error:?}")
                });
            conn.execute("INSERT INTO t VALUES (2);").await.unwrap();
            conn.close().await.unwrap();
        });
        joiner.finish();
        assert_eq!(count_rows(&db), 2);
    }

    #[test]
    fn read_write_open_beside_fsqlite_peers_is_admitted() {
        if std::env::var(ROLE).is_ok() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("fsqlite-peer.db");
        seed(&db);
        for (round, role) in ["fsqlite_rw", "fsqlite_ro"].into_iter().enumerate() {
            let peer = Peer::spawn(role, &db);
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(db.to_str().unwrap()).await.unwrap_or_else(|error| {
                    panic!("open beside an idle {role} peer must be admitted: {error:?}")
                });
                conn.execute("INSERT INTO t VALUES (2);").await.unwrap();
                let rows = conn.query_row("SELECT count(*) FROM t;").await.unwrap();
                assert_eq!(
                    rows.values(),
                    &[SqliteValue::Integer(i64::try_from(round).unwrap() + 2)]
                );
                conn.close().await.unwrap();
            });
            peer.finish();
        }
        assert_eq!(count_rows(&db), 3);
    }
}
