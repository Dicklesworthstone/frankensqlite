//! GH#384: `PRAGMA wal_checkpoint(TRUNCATE)` must never stamp the database
//! header change counter (bytes 24..28 / 92..96) with a value BELOW the one
//! carried by the page-1 image it backfills from the WAL.
//!
//! Shape: a long-lived primary connection `A` does a little setup and one
//! read, then goes idle. A second in-process connection `B` bulk-loads and is
//! closed. `A` then truncates the WAL. Before the fix the checkpoint writer
//! re-stamped page 1 from `A`'s own (stale, never refreshed) commit clock,
//! overwriting the higher counter that `B`'s last page-1 frame carried. With
//! the WAL now empty every connection in the process re-derived its clock from
//! that stale header, so its `BEGIN` snapshot sat below the commit sequences
//! still held for `B`'s pages in the process-shared `CommitIndex`, and every
//! write to those pages was rejected with
//! `database is busy (snapshot conflict on pages: N)` for the rest of the
//! process's life.
//!
//! The load-bearing assertions are the post-checkpoint writes on `A` and on a
//! fresh connection `C`; the header check is the mechanism-level witness.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const SETUP_TXNS: usize = 3;
const SETUP_ROWS: usize = 20;
const BULK_TXNS: usize = 24;
const BULK_ROWS: usize = 200;
const AFTER_TXNS: usize = 2;
const AFTER_ROWS: usize = 50;

async fn open(path: &str) -> Connection {
    let conn = Connection::open(path).await.expect("open connection");
    conn.query("PRAGMA journal_mode = WAL;")
        .await
        .expect("switch to WAL");
    conn.execute("PRAGMA wal_autocheckpoint = 0;")
        .await
        .expect("disable autocheckpoint");
    conn
}

async fn insert_txns(conn: &Connection, label: &str, txns: usize, rows: usize) {
    for tx in 1..=txns {
        conn.execute("BEGIN;").await.expect("BEGIN");
        for i in 0..rows {
            let key = tx * 1000 + i;
            conn.execute(&format!(
                "INSERT INTO t(k, payload) VALUES ({key}, zeroblob(900));"
            ))
            .await
            .unwrap_or_else(|error| {
                panic!("[{label}] txn {tx}: INSERT must succeed after the checkpoint (GH#384): {error}")
            });
        }
        conn.execute("COMMIT;").await.expect("COMMIT");
    }
}

fn header_change_counter(path: &str) -> u32 {
    let bytes = std::fs::read(path).expect("read database file");
    u32::from_be_bytes([bytes[24], bytes[25], bytes[26], bytes[27]])
}

fn version_valid_for(path: &str) -> u32 {
    let bytes = std::fs::read(path).expect("read database file");
    u32::from_be_bytes([bytes[92], bytes[93], bytes[94], bytes[95]])
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
fn gh384_truncate_from_idle_connection_keeps_wal_change_counter() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db = dir.path().join("x.db");
        let db_str = db.to_string_lossy().into_owned();

        // A: the application's long-lived primary connection.
        let a = open(&db_str).await;
        a.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER, payload BLOB);")
            .await
            .expect("create table");
        insert_txns(&a, "A setup", SETUP_TXNS, SETUP_ROWS).await;
        // One autocommit read leaves A holding a pinned read snapshot; A then
        // goes idle while B commits.
        assert_eq!(count_rows(&a).await, (SETUP_TXNS * SETUP_ROWS) as i64);

        // B: the application's bulk writer. Closed WITHOUT a checkpoint so the
        // only page-1 image carrying B's commits lives in the WAL.
        let b = open(&db_str).await;
        insert_txns(&b, "B bulk", BULK_TXNS, BULK_ROWS).await;
        b.close_without_checkpoint()
            .await
            .expect("close bulk writer");

        // A truncates the WAL without having run any statement since B's
        // last commit.
        let status = a
            .query("PRAGMA wal_checkpoint(TRUNCATE);")
            .await
            .expect("wal_checkpoint(TRUNCATE)");
        assert_eq!(
            status[0].values()[0],
            SqliteValue::Integer(0),
            "checkpoint must not report busy: {status:?}"
        );
        let wal_len = std::fs::metadata(dir.path().join("x.db-wal"))
            .map(|m| m.len())
            .unwrap_or(0);
        assert!(
            wal_len <= 32,
            "TRUNCATE must leave an empty WAL (got {wal_len} bytes)"
        );

        // Mechanism witness: every commit bumps the counter once, so the
        // header must carry at least (CREATE TABLE + every committed txn).
        let expected_min = (1 + SETUP_TXNS + BULK_TXNS) as u32;
        let stamped = header_change_counter(&db_str);
        assert!(
            stamped >= expected_min,
            "GH#384: checkpoint stamped header change counter {stamped} below the \
             WAL page-1 image's counter (expected at least {expected_min})"
        );
        assert_eq!(
            version_valid_for(&db_str),
            stamped,
            "version-valid-for (92..96) must match the change counter (24..28)"
        );

        // The user-visible contract: writes keep working on the checkpointing
        // connection and on a fresh connection opened after the reset.
        insert_txns(&a, "A after checkpoint", AFTER_TXNS, AFTER_ROWS).await;
        let c = open(&db_str).await;
        insert_txns(&c, "C fresh after checkpoint", AFTER_TXNS, AFTER_ROWS).await;

        let expected_total =
            (SETUP_TXNS * SETUP_ROWS + BULK_TXNS * BULK_ROWS + 2 * AFTER_TXNS * AFTER_ROWS) as i64;
        assert_eq!(count_rows(&c).await, expected_total);
        assert_eq!(count_rows(&a).await, expected_total);

        c.close().await.expect("close C");
        a.close().await.expect("close A");
    });
}

/// GH#385 companion: reset-mode checkpoints are no longer downgraded to FULL
/// just because another in-process connection is open. The remaining safety
/// question is the idle peer itself: `A` runs an autocommit read (pinning a
/// WAL read snapshot), then goes idle while `B` commits and truncates the WAL.
/// `A` must then observe every committed row and remain able to write; a
/// stale pinned snapshot on the reset WAL generation would surface here as a
/// wrong count, a read error, or a permanent snapshot conflict.
#[test]
fn gh385_idle_peer_with_pinned_snapshot_survives_peer_truncate() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db = dir.path().join("peer.db");
        let db_str = db.to_string_lossy().into_owned();

        let a = open(&db_str).await;
        a.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER, payload BLOB);")
            .await
            .expect("create table");
        insert_txns(&a, "A setup", SETUP_TXNS, SETUP_ROWS).await;
        // Pin a read snapshot on A, then leave it idle.
        assert_eq!(count_rows(&a).await, (SETUP_TXNS * SETUP_ROWS) as i64);

        let b = open(&db_str).await;
        insert_txns(&b, "B bulk", BULK_TXNS, BULK_ROWS).await;
        let status = b
            .query("PRAGMA wal_checkpoint(TRUNCATE);")
            .await
            .expect("wal_checkpoint(TRUNCATE) from B with idle peer A");
        assert_eq!(
            status[0].values()[0],
            SqliteValue::Integer(0),
            "an idle peer must not make the checkpoint busy (GH#385): {status:?}"
        );
        let wal_len = std::fs::metadata(dir.path().join("peer.db-wal"))
            .map(|m| m.len())
            .unwrap_or(0);
        assert!(
            wal_len <= 32,
            "TRUNCATE with an idle peer must reset the WAL (got {wal_len} bytes)"
        );

        // The idle peer must see B's commits through the reset generation and
        // must still be able to write.
        let committed = (SETUP_TXNS * SETUP_ROWS + BULK_TXNS * BULK_ROWS) as i64;
        assert_eq!(count_rows(&a).await, committed);
        insert_txns(&a, "A after peer truncate", AFTER_TXNS, AFTER_ROWS).await;
        insert_txns(&b, "B after truncate", AFTER_TXNS, AFTER_ROWS).await;
        let expected_total = committed + (2 * AFTER_TXNS * AFTER_ROWS) as i64;
        assert_eq!(count_rows(&a).await, expected_total);
        assert_eq!(count_rows(&b).await, expected_total);

        b.close().await.expect("close B");
        a.close().await.expect("close A");
    });
}
