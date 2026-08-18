#![recursion_limit = "512"]

//! bd-6n5cy stock-fidelity: SQLite refuses to ATTACH a database whose text
//! encoding differs from the main database, failing with SQLITE_ERROR
//! ("attached databases must use the same text encoding as main database").
//!
//! FrankenSQLite's decode layer *can* read a UTF-16 database (bd-bld9w), so it
//! previously accepted a cross-encoding attachment that stock rejects. This
//! pins the rejection (and confirms a same-encoding ATTACH still works).
//!
//! The attached databases are built with rusqlite (real SQLite 3.46.1) so their
//! on-disk encoding is authoritative.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn make_stock_db(path: &std::path::Path, encoding_pragma: Option<&str>, seed_value: &str) {
    let conn = rusqlite::Connection::open(path).unwrap();
    // `PRAGMA encoding` must precede the first write to take effect.
    let mut sql = String::new();
    if let Some(enc) = encoding_pragma {
        sql.push_str(&format!("PRAGMA encoding='{enc}';"));
    }
    sql.push_str(&format!(
        "CREATE TABLE a(y); INSERT INTO a VALUES('{seed_value}');"
    ));
    conn.execute_batch(&sql).unwrap();
}

#[test]
fn cross_encoding_attach_rejected_same_encoding_ok_gh6n5cy() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let utf16_db = dir.path().join("att_utf16.db");
        let utf8_db = dir.path().join("att_utf8.db");
        make_stock_db(&utf16_db, Some("UTF-16le"), "yo");
        make_stock_db(&utf8_db, None, "ok"); // default UTF-8

        // frank main is UTF-8 by default.
        let main_db = dir.path().join("main_utf8.db");
        let f = Connection::open(main_db.to_str().unwrap()).await.unwrap();
        f.execute("CREATE TABLE m(x)").await.unwrap();

        // Cross-encoding ATTACH (UTF-16 into UTF-8 main) must be REJECTED.
        let err = f
            .execute(&format!("ATTACH '{}' AS att16", utf16_db.display()))
            .await
            .expect_err("cross-encoding ATTACH must be rejected");
        let msg = format!("{err:?}").to_ascii_lowercase();
        assert!(
            msg.contains("same text encoding"),
            "expected the stock encoding-mismatch error, got: {err:?}"
        );

        // Same-encoding ATTACH (UTF-8 into UTF-8 main) must SUCCEED and read.
        f.execute(&format!("ATTACH '{}' AS att8", utf8_db.display()))
            .await
            .expect("same-encoding ATTACH must succeed");
        let rows = f.query("SELECT y FROM att8.a").await.unwrap();
        assert_eq!(rows.len(), 1, "attached UTF-8 db should read back one row");
        assert_eq!(
            rows[0].values().first(),
            Some(&SqliteValue::Text("ok".into())),
            "attached UTF-8 db row value"
        );
    });
}
