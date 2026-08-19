//! bd-lzbku: ATTACH of a brand-new/empty file to a UTF-16 main adopts main's
//! encoding IN MEMORY but must DEFER the on-disk page-1 header write to the aux's
//! first real write (like stock, which leaves an untouched aux at 0 bytes).
//!
//! Harm this guards: the pre-fix eager `update_database_header_metadata` at ATTACH
//! materialized a still-schema-empty aux with a committed UTF-16 encoding. Stock
//! leaves 0 bytes (adoptable anywhere); frank's materialized image reads as UTF-16
//! standalone, which a later cross-encoding attach (or stock) then rejects.
//!
//! (The read-only-main harm — ATTACH of an empty file failing because the aux is
//! opened schema-only and the eager persist needs a write txn — is guarded in code
//! by the `!attached_connection.pager.is_readonly()` gate on the deferred set.)

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn scalar_text(rows: &[fsqlite_core::connection::Row]) -> String {
    assert_eq!(rows.len(), 1, "expected one row");
    match &rows[0].values()[0] {
        SqliteValue::Text(s) => s.to_string(),
        other => panic!("expected TEXT, got {other:?}"),
    }
}

// Harm 1: an empty aux ATTACHed to a UTF-16 main but NEVER written must NOT be
// materialized on disk with a committed UTF-16 encoding. A standalone reopen must
// report UTF-8 (the untouched default), exactly like stock's 0-byte aux.
#[test]
fn bd_lzbku_unwritten_empty_aux_is_not_materialized() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let main_path = dir.path().join("main16.db");
        let aux_path = dir.path().join("aux_untouched.db");
        let main_str = main_path.to_string_lossy().into_owned();
        let aux_str = aux_path.to_string_lossy().into_owned();

        let conn = Connection::open(&main_str).await.unwrap();
        conn.execute("PRAGMA encoding = 'UTF-16le';").await.unwrap();
        conn.execute("CREATE TABLE m(a);").await.unwrap();

        // Empty cross-encoding ATTACH succeeds (adopts main's UTF-16 in memory)...
        conn.execute(&format!("ATTACH '{aux_str}' AS aux;"))
            .await
            .unwrap_or_else(|e| panic!("empty cross-encoding ATTACH must succeed: {e:?}"));
        // ...but we NEVER write to aux, so the persist must not have fired.
        conn.execute("DETACH aux;").await.unwrap();
        conn.close().await.unwrap();

        // Standalone reopen: an untouched aux reads as UTF-8, NOT a materialized
        // UTF-16le image. (Pre-fix this reported UTF-16le.)
        let stock = rusqlite::Connection::open(&aux_path).unwrap();
        let aux_encoding: String = stock
            .query_row("PRAGMA encoding;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            aux_encoding, "UTF-8",
            "an unwritten empty aux must not be eagerly materialized as UTF-16"
        );
    });
}

// bd-ntuz0 (a) regression / deferral-still-persists: once the aux takes its FIRST
// write, the deferred adopted encoding must be stamped into its header so a
// standalone reopen reports UTF-16le and reads the row.
#[test]
fn bd_lzbku_first_write_persists_deferred_encoding() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let main_path = dir.path().join("main16.db");
        let aux_path = dir.path().join("aux_written.db");
        let main_str = main_path.to_string_lossy().into_owned();
        let aux_str = aux_path.to_string_lossy().into_owned();

        let conn = Connection::open(&main_str).await.unwrap();
        conn.execute("PRAGMA encoding = 'UTF-16le';").await.unwrap();
        conn.execute("CREATE TABLE m(a);").await.unwrap();
        conn.execute(&format!("ATTACH '{aux_str}' AS aux;"))
            .await
            .unwrap_or_else(|e| panic!("empty cross-encoding ATTACH must succeed: {e:?}"));
        // First write flushes the deferred encoding into the aux header.
        conn.execute("CREATE TABLE aux.x(y TEXT);").await.unwrap();
        conn.execute("INSERT INTO aux.x VALUES ('café');")
            .await
            .unwrap();
        assert_eq!(
            scalar_text(&conn.query("SELECT y FROM aux.x;").await.unwrap()),
            "café",
            "attached row round-trips live"
        );
        conn.close().await.unwrap();

        let stock = rusqlite::Connection::open(&aux_path).unwrap();
        let aux_encoding: String = stock
            .query_row("PRAGMA encoding;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            aux_encoding, "UTF-16le",
            "first write must persist the deferred adopted UTF-16le encoding"
        );
        let integrity: String = stock
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(integrity, "ok", "stock integrity_check on the written aux");
        let val: String = stock
            .query_row("SELECT y FROM x;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "café", "stock reads the attached-then-written row");
    });
}
