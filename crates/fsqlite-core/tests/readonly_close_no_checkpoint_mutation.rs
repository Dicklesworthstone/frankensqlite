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
