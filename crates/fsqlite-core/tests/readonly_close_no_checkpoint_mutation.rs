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
            before, after,
            "bd-lcuoc: a read-only connection's close must not checkpoint/mutate \
             the main-DB bytes ({} bytes before, {} after)",
            before.len(),
            after.len()
        );

        writer.close().await.expect("writer close");
    });
}
