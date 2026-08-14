//! bd-daqmp: read-only opens of a HOT WAL family must be byte-neutral.
//!
//! Downstream (mcp_agent_mail_rust) guards forensic/salvage surfaces with a
//! strict contract: opening a database family read-only must not rewrite the
//! main file or the WAL — stock `sqlite3 mode=ro` on a hot-WAL family leaves
//! db+wal pristine (only the -shm may be rebuilt). The regression fired when
//! a family was left HOT (staged frames, no clean close) and any read-only
//! surface published/settled at open, rewriting the main file. Root cause was
//! the AsyncConnection drop-time checkpoint (fixed in f0f119a38); this keeper
//! pins the whole contract at HEAD: hot family -> RO open + SELECT -> main
//! and -wal byte-identical.

use fsqlite::Connection;
use fsqlite::compat::{OpenFlags, open_with_flags};

fn file_digest(path: &std::path::Path) -> Option<(u64, u64)> {
    let data = std::fs::read(path).ok()?;
    // Cheap stable digest: length + FNV-1a.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in &data {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    Some((data.len() as u64, hash))
}

#[test]
fn readonly_open_of_hot_wal_family_is_byte_neutral() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("daqmp_hot.db");
    let db = db_path.to_string_lossy().into_owned();
    let wal_path = temp_dir.path().join("daqmp_hot.db-wal");

    // Phase 1: build a HOT family — schema DDL committed into the WAL, then
    // DROP the connection without close(): no checkpoint, frames stay staged
    // in the WAL (the shape downstream quarantines/salvages).
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(&db).await.expect("open writer");
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;\n             CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT);\n             CREATE INDEX idx_t_data ON t(data);\n             INSERT INTO t VALUES (1, 'hot');",
        )
        .await
        .expect("hot-family setup");
        drop(conn); // simulated crash / abandoned handle — NO close()
    });

    let main_before = file_digest(&db_path).expect("main file must exist");
    let wal_before = file_digest(&wal_path);
    assert!(
        wal_before.is_some_and(|(len, _)| len > 0),
        "family must be HOT: the WAL must hold the staged frames"
    );

    // Phase 2: every read-only surface must leave main + wal untouched.
    asupersync::test_utils::run_test(|| async {
        let readonly = open_with_flags(&db, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .await
            .expect("read-only open of hot family");
        let rows = readonly
            .query("SELECT COUNT(*) FROM t;")
            .await
            .expect("read-only SELECT through the hot WAL");
        assert_eq!(rows.len(), 1, "RO reader must see the committed WAL state");
        drop(readonly);
    });

    let main_after = file_digest(&db_path).expect("main file must exist");
    let wal_after = file_digest(&wal_path);
    assert_eq!(
        main_before, main_after,
        "read-only open must not rewrite the MAIN file of a hot family \
         (stock mode=ro contract; bd-daqmp)"
    );
    assert_eq!(
        wal_before, wal_after,
        "read-only open must not rewrite the WAL of a hot family \
         (stock mode=ro contract; bd-daqmp)"
    );
}
