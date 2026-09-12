//! bd-lcuoc: a connection that performed NO durable writes must not mutate the
//! main-DB bytes at close. The close-time passive checkpoint is opportunistic WAL
//! hygiene (a WAL-preserving close is fully correct), so a read-only consumer's
//! close must leave `<db>` byte-identical — MTDT's source-integrity law.
//!
//! Repro shape: a writer seeds a WAL-mode DB and stays open so the WAL retains
//! frames; a second connection opens read-write, only SELECTs, then closes.
//! Its close must not checkpoint (it never wrote), so the main-DB file is
//! byte-stable across the read-only session.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn db_bytes(path: &str) -> Vec<u8> {
    std::fs::read(path).expect("read main db file")
}

fn wal_len(path: &str) -> u64 {
    std::fs::metadata(format!("{path}-wal"))
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}

#[cfg(all(unix, feature = "native"))]
#[test]
fn wal_retirement_refuses_staged_and_committed_frames_without_mutation() {
    use fsqlite_core::wal_adapter::WalBackendAdapter;
    use fsqlite_pager::WalBackend;
    use fsqlite_types::cx::Cx;
    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_vfs::{UnixVfs, Vfs};
    use fsqlite_wal::{WalFile, WalSalts};

    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let cx = Cx::new();
        let vfs = UnixVfs::new();
        for with_frame in [false, true] {
            let path = dir.path().join(format!("retirement-{with_frame}.wal"));
            let (file, _) = vfs
                .open(
                    &cx,
                    Some(&path),
                    VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE | VfsOpenFlags::WAL,
                )
                .unwrap();
            let wal = WalFile::create(
                &cx,
                file,
                4096,
                0,
                WalSalts {
                    salt1: 17,
                    salt2: 23,
                },
            )
            .await
            .unwrap();
            let mut backend = WalBackendAdapter::new(wal);
            if with_frame {
                backend.append_frame(&cx, 1, &[7; 4096], 1).await.unwrap();
                let before = std::fs::read(&path).unwrap();
                assert!(before.len() > 32);
                for committed in [false, true] {
                    if committed {
                        backend.sync(&cx).unwrap();
                    }
                    assert!(matches!(
                        backend.validate_empty_wal_for_retirement(&cx).await,
                        Err(fsqlite_error::FrankenError::Busy)
                    ));
                    assert!(matches!(
                        backend.retire_empty_wal(&cx).await,
                        Err(fsqlite_error::FrankenError::Busy)
                    ));
                    assert_eq!(std::fs::read(&path).unwrap(), before);
                }
            } else {
                assert_eq!(std::fs::metadata(&path).unwrap().len(), 32);
                for _ in 0..2 {
                    backend.validate_empty_wal_for_retirement(&cx).await.unwrap();
                    backend.retire_empty_wal(&cx).await.unwrap();
                    assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
                }
            }
        }
    });
}

#[test]
fn wal_to_delete_releases_lifetime_fence_for_foreign_stock_writer() {
    const CHILD_PATH: &str = "FSQLITE_WAL_TO_DELETE_STOCK_WRITER";
    const TEST: &str = "wal_to_delete_releases_lifetime_fence_for_foreign_stock_writer";
    if let Some(path) = std::env::var_os(CHILD_PATH) {
        let stock = rusqlite::Connection::open(std::path::Path::new(&path)).unwrap();
        stock.busy_timeout(std::time::Duration::ZERO).unwrap();
        let mode: String = stock
            .query_row("PRAGMA journal_mode;", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            mode, "delete",
            "stock writer must exercise rollback locking"
        );
        stock
            .execute_batch("BEGIN IMMEDIATE; INSERT INTO t VALUES (2); COMMIT;")
            .expect("an idle rollback-mode peer must not retain the WAL lifetime fence");
        return;
    }
    asupersync::test_utils::run_test(|| async {
        for with_idle_peer in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = dir.path().join("wal-to-delete.db");
            let conn = Connection::open(db.to_str().unwrap()).await.unwrap();
            conn.execute("PRAGMA journal_mode=WAL;").await.unwrap();
            conn.execute("CREATE TABLE t(x INTEGER);").await.unwrap();
            conn.execute("INSERT INTO t VALUES (1);").await.unwrap();
            assert!(wal_len(db.to_str().unwrap()) > 32);
            let peer = if with_idle_peer {
                let peer = Connection::open(db.to_str().unwrap()).await.unwrap();
                assert_eq!(
                    peer.query_row("SELECT x FROM t;").await.unwrap().values(),
                    &[SqliteValue::Integer(1)]
                );
                Some(peer)
            } else {
                None
            };
            if let Some(peer) = &peer {
                peer.execute("BEGIN;").await.unwrap();
                peer.query_row("SELECT x FROM t;").await.unwrap();
                let before = db_bytes(db.to_str().unwrap());
                let error = conn
                    .query_row("PRAGMA journal_mode=DELETE;")
                    .await
                    .expect_err("a pinned peer must prevent the mode transition");
                assert!(matches!(error, fsqlite_error::FrankenError::Busy));
                assert_eq!(db_bytes(db.to_str().unwrap()), before);
                assert!(wal_len(db.to_str().unwrap()) > 32);
                peer.execute("ROLLBACK;").await.unwrap();
            }
            let mode = conn
                .query_row("PRAGMA journal_mode=DELETE;")
                .await
                .unwrap();
            assert_eq!(mode.values(), &[SqliteValue::Text("delete".into())]);
            if let Some(peer) = &peer {
                // Every attached handle explicitly leaves WAL mode. A peer
                // observing the already-published rollback mode must still
                // detach its own lifetime claim through the same-mode path.
                let mode = peer
                    .query_row("PRAGMA journal_mode=DELETE;")
                    .await
                    .unwrap();
                assert_eq!(mode.values(), &[SqliteValue::Text("delete".into())]);
            }
            assert_eq!(&db_bytes(db.to_str().unwrap())[18..20], &[1, 1]);
            assert_eq!(
                wal_len(db.to_str().unwrap()),
                0,
                "retire the WAL header too"
            );
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([TEST, "--exact", "--nocapture"])
                .env(CHILD_PATH, &db)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "stock writer failed (idle_peer={with_idle_peer}): {output:?}"
            );
            let rows = conn.query("SELECT x FROM t ORDER BY x;").await.unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));
            assert_eq!(rows[1].values()[0], SqliteValue::Integer(2));
            if let Some(peer) = peer {
                assert_eq!(
                    peer.query_row("SELECT count(*) FROM t;")
                        .await
                        .unwrap()
                        .values(),
                    &[SqliteValue::Integer(2)]
                );
                peer.close().await.unwrap();
            }
            // The retired adapter cannot be reused: re-entering WAL installs
            // a fresh header/backend and must retain the stock writer's row.
            let mode = conn.query_row("PRAGMA journal_mode=WAL;").await.unwrap();
            assert_eq!(mode.values(), &[SqliteValue::Text("wal".into())]);
            conn.execute("INSERT INTO t VALUES (3);").await.unwrap();
            assert_eq!(
                conn.query_row("SELECT count(*) FROM t;")
                    .await
                    .unwrap()
                    .values(),
                &[SqliteValue::Integer(3)]
            );
            conn.close().await.unwrap();
            let stock = rusqlite::Connection::open(&db).unwrap();
            let count: i64 = stock
                .query_row("SELECT count(*) FROM t;", [], |row| row.get(0))
                .unwrap();
            assert_eq!(count, 3);
        }
    });
}

#[test]
fn readonly_connection_close_does_not_mutate_main_db_bytes() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db = dir.path().join("library.db").to_string_lossy().into_owned();

        // Writer: seed a WAL-mode DB, commit rows, and keep the connection open
        // so the WAL is not truncated out from under the reader.
        let writer = Connection::open(&db).await.expect("open writer");
        writer
            .execute("PRAGMA journal_mode=WAL;")
            .await
            .expect("wal mode");
        writer
            .execute("CREATE TABLE evidence (id INTEGER PRIMARY KEY, piece TEXT);")
            .await
            .expect("create");
        for i in 0..64 {
            writer
                .execute(&format!(
                    "INSERT INTO evidence (id, piece) VALUES ({i}, 'piece-{i:04}');"
                ))
                .await
                .expect("insert");
        }

        // A WAL must exist for a checkpoint to have anything to fold in.
        assert!(
            std::path::Path::new(&format!("{db}-wal")).exists(),
            "precondition: writer must leave a -wal file with frames"
        );

        // Reader: opens read-write but only reads, then closes.
        let before = db_bytes(&db);
        let reader = Connection::open(&db).await.expect("open reader");
        let rows = reader
            .query("SELECT COUNT(*) FROM evidence;")
            .await
            .expect("read count");
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Integer(64),
            "reader must see the seeded rows"
        );
        let _ = reader
            .query("SELECT id, piece FROM evidence WHERE id < 8 ORDER BY id;")
            .await
            .expect("read rows");
        reader.close().await.expect("reader close");

        let after = db_bytes(&db);
        assert_eq!(
            before,
            after,
            "bd-lcuoc: a read-only connection's close must not checkpoint/mutate \
             the main-DB bytes ({} bytes before, {} after)",
            before.len(),
            after.len()
        );

        writer.close().await.expect("writer close");
    });
}

#[cfg(all(unix, feature = "native"))]
#[test]
fn strict_readonly_reopens_while_same_process_wal_writer_remains_alive() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db = dir.path().join("same-process-readonly.db");
        let path = db.to_str().expect("UTF-8 database path");
        let writer = Connection::open(path).await.expect("open writer");
        writer
            .execute("PRAGMA journal_mode=WAL; PRAGMA fsqlite.stmt_microbatch=OFF;")
            .await
            .expect("configure WAL writer");
        writer
            .execute("CREATE TABLE evidence(id INTEGER PRIMARY KEY, piece TEXT);")
            .await
            .expect("create evidence");
        writer
            .execute("INSERT INTO evidence VALUES (1, 'committed');")
            .await
            .expect("seed evidence");
        for pinned_writer_read in [false, true] {
            if pinned_writer_read {
                writer.execute("BEGIN;").await.expect("begin writer read");
            }
            assert_eq!(
                writer.query_row("SELECT piece FROM evidence WHERE id=1;")
                    .await.expect("writer reads committed row").values(),
                &[SqliteValue::Text("committed".into())]
            );
            let before = db_bytes(path);
            for attempt in 0..8 {
                // SQLITE_OPEN_READ_ONLY routes to this exact core constructor.
                let reader = Connection::open_schema_only(path).await.unwrap_or_else(|error| {
                    panic!("strict read-only open {attempt} failed with live writer (pinned={pinned_writer_read}): {error:?}")
                });
                assert_eq!(
                    reader.query_row("SELECT piece FROM evidence WHERE id=1;")
                        .await.expect("strict reader reads committed row").values(),
                    &[SqliteValue::Text("committed".into())]
                );
                reader.execute("INSERT INTO evidence VALUES (2, 'refused');")
                    .await.expect_err("strict reader must reject writes");
                reader.close().await.expect("close strict reader");
                assert_eq!(db_bytes(path), before, "strict reader changed the database");
            }
            if pinned_writer_read {
                writer.execute("ROLLBACK;").await.expect("end writer read");
            }
        }
        writer.close().await.expect("close writer");
    });
}

/// GH #384: checkpointing from an idle connection must retain the newer
/// page-1 change counter written to the WAL by a peer connection. Otherwise a
/// successful TRUNCATE reset leaves both existing and newly opened in-process
/// connections permanently below the process-shared MVCC commit index.
#[test]
fn stale_connection_truncate_checkpoint_preserves_latest_header_counter() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db = dir
            .path()
            .join("stale-checkpoint-counter.db")
            .to_string_lossy()
            .into_owned();

        let checkpointer = Connection::open(&db).await.expect("open checkpointer");
        checkpointer
            .execute("PRAGMA journal_mode=WAL;")
            .await
            .expect("enable WAL");
        checkpointer
            .execute("PRAGMA wal_autocheckpoint=0;")
            .await
            .expect("disable autocheckpoint");
        checkpointer
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT NOT NULL);")
            .await
            .expect("create table");
        checkpointer
            .execute("INSERT INTO t VALUES (1, 'checkpointer');")
            .await
            .expect("seed table");
        checkpointer
            .query("SELECT COUNT(*) FROM t;")
            .await
            .expect("pin checkpointer snapshot");
        let stale_counter = checkpointer.change_counter().await;

        let writer = Connection::open(&db).await.expect("open writer");
        writer
            .execute("PRAGMA journal_mode=WAL;")
            .await
            .expect("retain WAL mode");
        writer
            .execute("PRAGMA wal_autocheckpoint=0;")
            .await
            .expect("disable writer autocheckpoint");
        for id in 2..=9 {
            writer
                .execute(&format!(
                    "INSERT INTO t VALUES ({id}, '{}');",
                    "x".repeat(900)
                ))
                .await
                .expect("writer commit");
        }
        let writer_counter = writer.change_counter().await;
        assert!(
            writer_counter > stale_counter,
            "test requires the writer to advance beyond the checkpointer's pinned clock"
        );
        // Match the reported bulk-writer lifetime: an awaited close may run
        // maintenance work that refreshes or checkpoints the durable horizon.
        drop(writer);

        let checkpoint_rows = checkpointer
            .query("PRAGMA wal_checkpoint(TRUNCATE);")
            .await
            .expect("truncate checkpoint");
        assert_eq!(checkpoint_rows[0].values()[0], SqliteValue::Integer(0));

        let database = db_bytes(&db);
        let checkpoint_counter = u32::from_be_bytes(database[24..28].try_into().unwrap());
        let version_valid_for = u32::from_be_bytes(database[92..96].try_into().unwrap());
        assert!(
            checkpoint_counter >= writer_counter,
            "GH #384: checkpoint lowered header counter from {writer_counter} to {checkpoint_counter}"
        );
        assert_eq!(version_valid_for, checkpoint_counter);

        checkpointer
            .execute("INSERT INTO t VALUES (10, 'after-checkpoint');")
            .await
            .expect("the stale checkpointer must remain able to write");
        let fresh = Connection::open(&db).await.expect("open fresh connection");
        fresh
            .execute("INSERT INTO t VALUES (11, 'fresh-connection');")
            .await
            .expect("a fresh in-process connection must remain able to write");
        let rows = fresh
            .query("SELECT COUNT(*) FROM t;")
            .await
            .expect("count rows");
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(11));
        fresh.close().await.expect("close fresh connection");
        checkpointer
            .close()
            .await
            .expect("close checkpointer connection");
    });
}

/// GH #385: an open but idle same-process connection does not hold a read
/// snapshot and must not prevent a reset-mode checkpoint from truncating WAL.
#[test]
fn truncate_checkpoint_resets_wal_with_idle_peer_connection() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db = dir
            .path()
            .join("idle-peer-checkpoint.db")
            .to_string_lossy()
            .into_owned();

        let writer = Connection::open(&db).await.expect("open writer");
        writer
            .execute("PRAGMA journal_mode=WAL;")
            .await
            .expect("enable WAL");
        writer
            .execute("PRAGMA wal_autocheckpoint=0;")
            .await
            .expect("disable autocheckpoint");
        writer
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT NOT NULL);")
            .await
            .expect("create table");

        let idle_peer = Connection::open(&db).await.expect("open idle peer");
        for id in 1..=16 {
            writer
                .execute(&format!(
                    "INSERT INTO t VALUES ({id}, '{}');",
                    "x".repeat(900)
                ))
                .await
                .expect("writer commit");
        }

        let before = wal_len(&db);
        assert!(before > 32, "test requires committed WAL frames");
        let checkpoint = writer
            .query("PRAGMA wal_checkpoint(TRUNCATE);")
            .await
            .expect("truncate checkpoint");
        assert_eq!(checkpoint[0].values()[0], SqliteValue::Integer(0));
        let after = wal_len(&db);
        assert!(
            after <= 32,
            "GH #385: idle peer left WAL at {after} bytes after reset-mode checkpoint (before {before})"
        );

        let rows = idle_peer
            .query("SELECT COUNT(*) FROM t;")
            .await
            .expect("idle peer reads reset generation");
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(16));
        idle_peer
            .execute("INSERT INTO t VALUES (17, 'idle-peer');")
            .await
            .expect("idle peer writes after reset");

        idle_peer.close().await.expect("close idle peer");
        writer.close().await.expect("close writer");
    });
}

/// GH #385: post-commit autocheckpointing must use transaction activity, not
/// the number of open handles, when deciding whether an idle peer is safe.
#[test]
fn autocheckpoint_backfills_with_idle_peer_connection() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db = dir
            .path()
            .join("idle-peer-autocheckpoint.db")
            .to_string_lossy()
            .into_owned();

        let writer = Connection::open(&db).await.expect("open writer");
        writer
            .execute("PRAGMA journal_mode=WAL;")
            .await
            .expect("enable WAL");
        writer
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT NOT NULL);")
            .await
            .expect("create table");
        writer
            .execute("PRAGMA wal_autocheckpoint=1;")
            .await
            .expect("enable frequent autocheckpoint");
        writer
            .query("PRAGMA checkpoint_write_pressure_fps=1000000000;")
            .await
            .expect("disable write-pressure delay");

        let before = std::fs::metadata(&db).expect("main db metadata").len();
        let idle_peer = Connection::open(&db).await.expect("open idle peer");
        for id in 1..=16 {
            writer
                .execute(&format!(
                    "INSERT INTO t VALUES ({id}, '{}');",
                    "y".repeat(900)
                ))
                .await
                .expect("writer commit");
        }

        let after = std::fs::metadata(&db).expect("main db metadata").len();
        assert!(
            after > before,
            "GH #385: autocheckpoint did not backfill with idle peer (main db stayed {after} bytes)"
        );
        assert!(wal_len(&db) > 32, "test requires a live WAL generation");

        idle_peer.close().await.expect("close idle peer");
        writer.close().await.expect("close writer");
    });
}
