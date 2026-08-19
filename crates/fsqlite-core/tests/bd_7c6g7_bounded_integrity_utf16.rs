//! bd-7c6g7 #6: the BOUNDED integrity check (`validate_database_integrity_bounded`)
//! is frank's own image-publication gate. It recomputed every expected index key
//! by decoding the row payload and serializing the key with the UTF-8-hardcoded
//! `parse_record` / `serialize_record`. On a UTF-16 database the stored index
//! keys are UTF-16, so every recomputed key diverged and the gate reported a
//! CLEAN image as corrupt. It now reads rows and serializes keys under the DB
//! storage encoding, so a clean UTF-16 image passes.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

#[test]
fn bounded_integrity_accepts_clean_utf16_indexed_database() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir
            .path()
            .join("u16.db")
            .to_string_lossy()
            .into_owned();
        let conn = Connection::open(&db).await.unwrap();

        // Set UTF-16 before any table exists, then build a rowid table with a
        // secondary index on a TEXT column and non-ASCII BMP rows whose UTF-16
        // byte layout diverges from UTF-8.
        conn.execute("PRAGMA encoding = 'UTF-16le';").await.unwrap();
        // The bounded whole-image gate only validates rollback/DELETE-mode
        // images (a test-local setting; not the concurrent default).
        conn.execute("PRAGMA journal_mode = DELETE;").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);")
            .await
            .unwrap();
        conn.execute("CREATE INDEX idx_name ON t(name);").await.unwrap();
        for (i, name) in ["café", "grüße", "日本語", "ascii"].iter().enumerate() {
            conn.execute(&format!("INSERT INTO t VALUES({}, '{}');", i + 1, name))
                .await
                .unwrap();
        }

        // Guard against a silent UTF-8 fallback that would make this test vacuous:
        // the database must actually be UTF-16.
        let enc = conn.query("PRAGMA encoding;").await.unwrap();
        let enc_text = match &enc[0].values()[0] {
            SqliteValue::Text(s) => s.as_ref().to_owned(),
            other => panic!("PRAGMA encoding returned non-text {other:?}"),
        };
        assert!(
            enc_text.eq_ignore_ascii_case("UTF-16le"),
            "fixture must be UTF-16le, got {enc_text}"
        );

        // The bounded gate must ACCEPT this clean image. Before the fix it
        // returned DatabaseCorrupt because the recomputed index keys were
        // UTF-8-serialized while the stored keys are UTF-16.
        conn.validate_database_integrity_bounded(dir.path())
            .await
            .expect("bounded integrity must accept a clean UTF-16 indexed database");
    });
}
