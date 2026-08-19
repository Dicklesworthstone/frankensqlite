//! bd-pgqsk L5 + M7: `PRAGMA encoding` setter fidelity and cross-encoding
//! ATTACH of an empty file.
//!
//! L5 — the encoding setter must accept every stock spelling (case-insensitive,
//! optional dash; `UTF-16` => UTF-16le), reject an unsupported name with
//! "unsupported encoding: <name>" (empty DB or not), and honour a valid choice
//! only on an empty database.
//!
//! M7 — attaching a brand-new / EMPTY file to a UTF-16 main must succeed (the
//! empty file adopts main's encoding), where a populated cross-encoding file is
//! still rejected.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn scalar_text(rows: &[fsqlite_core::connection::Row]) -> String {
    assert_eq!(rows.len(), 1, "expected one row");
    match &rows[0].values()[0] {
        SqliteValue::Text(s) => s.to_string(),
        other => panic!("expected TEXT, got {other:?}"),
    }
}

#[test]
fn bd_pgqsk_l5_encoding_setter_spellings_and_rejection() {
    asupersync::test_utils::run_test(|| async {
        // Every accepted spelling resolves like stock.
        for (spelling, expected) in [
            ("UTF-8", "UTF-8"),
            ("UTF8", "UTF-8"),
            ("utf8", "UTF-8"),
            ("UTF-16", "UTF-16le"),
            ("UTF16", "UTF-16le"),
            ("UTF-16le", "UTF-16le"),
            ("UTF16le", "UTF-16le"),
            ("UTF-16be", "UTF-16be"),
            ("UTF16be", "UTF-16be"),
        ] {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute(&format!("PRAGMA encoding = '{spelling}';"))
                .await
                .unwrap_or_else(|e| panic!("'{spelling}' should be accepted: {e:?}"));
            let got = scalar_text(&conn.query("PRAGMA encoding;").await.unwrap());
            assert_eq!(got, expected, "spelling '{spelling}' resolves to {expected}");
        }

        // An unsupported name is rejected (empty DB), matching stock.
        for bad in ["bogus", "UTF-32", "latin1"] {
            let conn = Connection::open(":memory:").await.unwrap();
            let err = conn
                .execute(&format!("PRAGMA encoding = '{bad}';"))
                .await
                .expect_err(&format!("'{bad}' must be rejected"));
            assert!(
                err.to_string().contains("unsupported encoding"),
                "'{bad}' error should mention unsupported encoding: {err}"
            );
        }
    });
}

#[test]
fn bd_pgqsk_l5_encoding_valid_name_noop_on_nonempty_db() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(a);").await.unwrap();
        // A VALID encoding on a non-empty DB is a silent no-op (never an error).
        conn.execute("PRAGMA encoding = 'UTF-16le';").await.unwrap();
        assert_eq!(
            scalar_text(&conn.query("PRAGMA encoding;").await.unwrap()),
            "UTF-8",
            "encoding cannot change on a non-empty DB"
        );
        // But an unsupported name still errors even on a non-empty DB.
        let err = conn
            .execute("PRAGMA encoding = 'bogus';")
            .await
            .expect_err("bogus must be rejected on a non-empty DB too");
        assert!(err.to_string().contains("unsupported encoding"), "got: {err}");
    });
}

#[test]
fn bd_pgqsk_m7_empty_attach_adopts_main_encoding() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let main_path = dir.path().join("main16.db");
        let aux_path = dir.path().join("aux_new.db");
        let main_str = main_path.to_string_lossy().into_owned();
        let aux_str = aux_path.to_string_lossy().into_owned();

        let conn = Connection::open(&main_str).await.unwrap();
        conn.execute("PRAGMA encoding = 'UTF-16le';").await.unwrap();
        conn.execute("CREATE TABLE m(a);").await.unwrap();

        // Attaching a brand-new EMPTY file must succeed — it adopts main's UTF-16.
        conn.execute(&format!("ATTACH '{aux_str}' AS aux;"))
            .await
            .unwrap_or_else(|e| panic!("empty cross-encoding ATTACH must succeed: {e:?}"));
        conn.execute("CREATE TABLE aux.x(y TEXT);").await.unwrap();
        conn.execute("INSERT INTO aux.x VALUES ('café');")
            .await
            .unwrap();
        let got = conn
            .query("SELECT y FROM aux.x;")
            .await
            .unwrap();
        assert_eq!(scalar_text(&got), "café", "attached row round-trips");
        conn.close().await.unwrap();

        // Stock reopens the aux file and reads it (written in main's encoding).
        let stock = rusqlite::Connection::open(&aux_path).unwrap();
        let integrity: String = stock
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(integrity, "ok", "stock integrity_check on the aux image");
        let val: String = stock
            .query_row("SELECT y FROM x;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(val, "café", "stock reads the attached-then-written row");
    });
}

/// bd-dbpl2: the encoding choice is re-settable while the database is EMPTY
/// (schema_cookie == 0) — a later `PRAGMA encoding` overrides the earlier one,
/// matching stock — but becomes one-way (locked) after the first schema object.
#[test]
fn bd_dbpl2_encoding_reset_on_empty_then_locked_after_schema() {
    asupersync::test_utils::run_test(|| async {
        // Empty DB: the last set wins (UTF-16le then UTF-8 => UTF-8).
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("PRAGMA encoding = 'UTF-16le';").await.unwrap();
        conn.execute("PRAGMA encoding = 'UTF-8';").await.unwrap();
        assert_eq!(
            scalar_text(&conn.query("PRAGMA encoding;").await.unwrap()),
            "UTF-8",
            "encoding is re-settable while the DB is empty"
        );

        // After a schema object it is locked: a further set is a silent no-op.
        let conn2 = Connection::open(":memory:").await.unwrap();
        conn2.execute("PRAGMA encoding = 'UTF-16le';").await.unwrap();
        conn2.execute("CREATE TABLE t(a);").await.unwrap();
        conn2.execute("PRAGMA encoding = 'UTF-8';").await.unwrap();
        assert_eq!(
            scalar_text(&conn2.query("PRAGMA encoding;").await.unwrap()),
            "UTF-16le",
            "encoding is locked once a schema object exists"
        );
    });
}
