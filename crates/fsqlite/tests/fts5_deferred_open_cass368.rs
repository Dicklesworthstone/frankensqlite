//! Regression for cass GH #368 defect 3: a database whose FTS5 `%_data` shadow
//! structure record is corrupt cannot be opened even to repair it, because the
//! schema reload eagerly reads + validates that record on every open. The
//! `open_existing_schema_only_deferred_fts5` family keeps the bare (empty) FTS5
//! vtab and skips the `%_data` read, so a repair path can drop+recreate the
//! corrupt shadow.
//!
//! Run: `cargo test -p fsqlite --features fts5 --test fts5_deferred_open_cass368`

#![cfg(feature = "fts5")]

use fsqlite::{Connection, SqliteValue};

/// The FTS5 structure record lives at `%_data` rowid 10.
const FTS5_STRUCTURE_ROWID: i64 = 10;

#[test]
fn deferred_fts5_open_survives_corrupt_shadow_structure() {
    asupersync::test_utils::run_test(|| async {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_owned();

        // Build a valid FTS5 table with data, then corrupt its structure record so
        // any decode at open fails.
        {
            let conn = Connection::open(&path).await.unwrap();
            conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
            conn.execute("CREATE VIRTUAL TABLE idx USING fts5(body, content='')")
                .await
                .expect("create contentless fts5");
            conn.execute_with_params(
                "INSERT INTO idx(rowid, body) VALUES (?1, ?2)",
                &[
                    SqliteValue::Integer(1),
                    SqliteValue::Text("hello world".into()),
                ],
            )
            .await
            .expect("insert fts row");

            // Overwrite the structure record with garbage: the leading varint now
            // decodes to a segment count far above the FTS5 maximum.
            conn.execute_with_params(
                "UPDATE idx_data SET block = ?1 WHERE id = ?2",
                &[
                    SqliteValue::Blob(std::sync::Arc::from([0xFFu8; 12].as_slice())),
                    SqliteValue::Integer(FTS5_STRUCTURE_ROWID),
                ],
            )
            .await
            .expect("overwrite fts5 structure record");
        }

        // A normal existing-schema open must FAIL: the reload reads + decodes the
        // now-corrupt structure record before any query touches the table.
        let normal = Connection::open_existing_schema_only(&path).await;
        assert!(
            normal.is_err(),
            "normal existing-schema open must fail on a corrupt FTS5 structure record"
        );
        let message = format!("{:#}", normal.err().unwrap());
        assert!(
            message.contains("%_data")
                || message.to_ascii_lowercase().contains("fts5")
                || message.contains("segment"),
            "expected an FTS5 %_data corruption error, got: {message}"
        );

        // The deferred-hydration repair open must SUCCEED: it keeps the bare FTS5
        // vtab and never reads %_data (#368 defect 3), so a repair path can now
        // drop + recreate the corrupt shadow.
        let repair = Connection::open_existing_schema_only_deferred_fts5(&path).await;
        assert!(
            repair.is_ok(),
            "deferred-fts5 open must succeed on a corrupt FTS5 shadow: {:?}",
            repair.err()
        );

        // And with the DB now open, the corrupt shadow is removable destructor-free
        // (the FTS5 vtab has a no-op destructor; the backing tables are plain).
        let conn = repair.unwrap();
        conn.execute("DROP TABLE idx")
            .await
            .expect("dropping the corrupt FTS5 table must not read %_data");
        conn.execute("CREATE VIRTUAL TABLE idx USING fts5(body, content='')")
            .await
            .expect("recreate a fresh FTS5 shadow after repair");
    });
}
