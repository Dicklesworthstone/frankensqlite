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
        let db = dir.path().join("u16.db").to_string_lossy().into_owned();
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
        conn.execute("CREATE INDEX idx_name ON t(name);")
            .await
            .unwrap();
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

/// bd-cquyy: the bounded FK walker seeks the parent UNIQUE index with
/// `index_move_to` (a raw b-tree byte seek). The parent probe was still built
/// with the UTF-8-hardcoded `serialize_record`, so on a UTF-16 database the
/// probe never matched the UTF-16-encoded parent index entry — the seek landed
/// wrong and a CLEAN image with a TEXT foreign key was reported corrupt. The
/// probe is now `serialize_record_with_encoding` under the DB encoding. Covers
/// both an ASCII and a non-ASCII parent key (the ASCII case regressed too,
/// because a UTF-16 'a' is `61 00`, not `61`).
#[test]
fn bounded_integrity_accepts_clean_utf16_foreign_key_database() {
    asupersync::test_utils::run_test(|| async {
        for parent_val in ["alpha", "café"] {
            let dir = tempfile::tempdir().unwrap();
            let db = dir.path().join("u16fk.db").to_string_lossy().into_owned();
            let conn = Connection::open(&db).await.unwrap();

            conn.execute("PRAGMA encoding = 'UTF-16le';").await.unwrap();
            conn.execute("PRAGMA journal_mode = DELETE;").await.unwrap();
            conn.execute("CREATE TABLE parent(name TEXT UNIQUE);")
                .await
                .unwrap();
            conn.execute("CREATE TABLE child(pname TEXT REFERENCES parent(name));")
                .await
                .unwrap();
            conn.execute(&format!("INSERT INTO parent VALUES('{parent_val}');"))
                .await
                .unwrap();
            conn.execute(&format!("INSERT INTO child VALUES('{parent_val}');"))
                .await
                .unwrap();

            // Guard against a silent UTF-8 fallback that would make this vacuous.
            let enc = conn.query("PRAGMA encoding;").await.unwrap();
            let enc_text = match &enc[0].values()[0] {
                SqliteValue::Text(s) => s.as_ref().to_owned(),
                other => panic!("PRAGMA encoding returned non-text {other:?}"),
            };
            assert!(
                enc_text.eq_ignore_ascii_case("UTF-16le"),
                "fixture must be UTF-16le, got {enc_text}"
            );

            // The FK relationship is satisfied (child.pname == parent.name), so
            // the bounded gate must ACCEPT the image. Before the fix the parent
            // seek probe (UTF-8) missed the UTF-16 parent entry and this returned
            // DatabaseCorrupt "FOREIGN KEY constraint ...".
            conn.validate_database_integrity_bounded(dir.path())
                .await
                .unwrap_or_else(|e| {
                    panic!(
                        "bounded integrity must accept a clean UTF-16 DB with a TEXT FK \
                         (parent='{parent_val}'), got {e:?}"
                    )
                });
        }
    });
}
