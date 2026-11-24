//! GH#538: an idle peer must allocate from the compacted image after VACUUM.
#![cfg(feature = "native")]

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

async fn page_count(conn: &Connection) -> i64 {
    let rows = conn.query("PRAGMA page_count").await.unwrap();
    match &rows[0].values()[0] {
        SqliteValue::Integer(count) => *count,
        value => panic!("unexpected page_count: {value:?}"),
    }
}

#[test]
fn idle_wal_writer_uses_compacted_extent_after_peer_vacuum() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("vacuum-peer.db");
        {
            let stock = rusqlite::Connection::open(&path).unwrap();
            stock
                .execute_batch(
                    "PRAGMA auto_vacuum=NONE;
                 PRAGMA journal_mode=WAL;
                 CREATE TABLE keep(id INTEGER PRIMARY KEY, val TEXT);
                 INSERT INTO keep VALUES (1, 'original');
                 CREATE TABLE discarded(payload BLOB);
                 INSERT INTO discarded VALUES (zeroblob(1048576));
                 DROP TABLE discarded;",
                )
                .unwrap();
        }

        let path_str = path.to_str().unwrap();
        let writer = Connection::open(path_str).await.unwrap();
        let maintenance = Connection::open(path_str).await.unwrap();
        writer
            .execute("UPDATE keep SET val = 'before' WHERE id = 1")
            .await
            .unwrap();
        maintenance
            .execute("PRAGMA wal_checkpoint(TRUNCATE)")
            .await
            .unwrap();
        // Establish the peer's old extent with an already empty WAL. VACUUM
        // can change the database without adding any frames to this index.
        let old_count = page_count(&writer).await;
        maintenance.execute("VACUUM").await.unwrap();
        let compact_count = page_count(&maintenance).await;
        assert!(old_count > compact_count + 64);
        assert_eq!(page_count(&writer).await, compact_count);

        let value = "x".repeat(2500);
        for id in 2..=13 {
            writer
                .execute(&format!("INSERT INTO keep VALUES ({id}, '{value}')"))
                .await
                .unwrap();
        }
        let grown_count = page_count(&writer).await;
        assert!(
            grown_count > compact_count && grown_count < old_count,
            "old={old_count} compact={compact_count} grown={grown_count}"
        );
        writer.close().await.unwrap();
        maintenance.close().await.unwrap();

        let stock = rusqlite::Connection::open(&path).unwrap();
        let integrity: String = stock
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .unwrap();
        assert_eq!(integrity, "ok");
        let (count, value): (i64, String) = stock
            .query_row(
                "SELECT count(*), (SELECT val FROM keep WHERE id = 1) FROM keep",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!((count, value.as_str()), (13, "before"));
    });
}
