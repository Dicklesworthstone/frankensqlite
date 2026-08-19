//! bd-f3s2l: `CAST(blob AS TEXT/INTEGER/REAL)` must relabel/decode the blob's
//! raw bytes via the DATABASE text encoding, not as canonical UTF-8. The
//! companion `utf16_invalid_text_translation_p9nhq_oracle.rs` pins the TEXT cast
//! on the INTERPRETED `apply_cast` path (INSERT ... VALUES). This file covers the
//! two remaining surfaces so the fix is verified on BOTH cast funnels:
//!
//!   1. The VDBE `sql_cast` path — reached by a `SELECT`-compiled cast
//!      (`INSERT INTO t SELECT CAST(v AS TEXT) FROM b`), so a blob stored in a
//!      UTF-16 DB round-trips through frank's VDBE cast and its stored TEXT image
//!      must match stock byte-for-byte.
//!   2. The numeric cast arms (`CAST(blob AS INTEGER/REAL)`), which in stock parse
//!      the blob's bytes in the DB encoding (`sqlite3Atoi64(..., enc)`); a UTF-16
//!      digit blob like X'31003200' ("12" LE) must read as 12, not 1.
//!
//! Oracle = rusqlite: for TEXT we compare frank's on-disk image (read back
//! through stock) to stock's own; for numerics we compare frank's query result to
//! stock's directly.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// TEXT-cast blobs (encoding-independent raw bytes), reused from the p9nhq set.
const TEXT_CASES: &[(&str, &str)] = &[
    ("ascii_A_41", "41"),
    ("ascii_AB_4142", "4142"),
    ("valid_u2002_e28082", "E28082"),
    ("valid_u0080_c280", "C280"),
    ("invalid_bare_cont_80", "80"),
    ("invalid_overlong_c0af", "C0AF"),
    ("invalid_surrogate_eda080", "EDA080"),
    ("invalid_overlong4_f08080af", "F08080AF"),
];

fn stock_read_col0_text(path: &std::path::Path, sql: &str) -> Vec<String> {
    let conn = rusqlite::Connection::open(path).expect("stock reopen");
    let mut stmt = conn.prepare(sql).expect("prepare stock");
    stmt.query_map([], |row| row.get::<_, String>(0))
        .expect("run stock")
        .collect::<std::result::Result<Vec<_>, _>>()
        .expect("collect stock")
}

fn header_text_encoding(path: &std::path::Path) -> u32 {
    let bytes = std::fs::read(path).expect("read header");
    u32::from_be_bytes([bytes[56], bytes[57], bytes[58], bytes[59]])
}

/// VDBE-path TEXT cast: `INSERT INTO t SELECT CAST(v AS TEXT) FROM b`. The blob is
/// stored verbatim in `b`, then a SELECT-compiled cast relabels it via the DB
/// encoding and stores the TEXT into `t`. Read both images back through stock.
async fn run_vdbe_text_pair(encoding: &str, expect_header: u32) {
    let dir = tempfile::TempDir::new().unwrap();
    let stock_path = dir.path().join("stock.db");
    let frank_path = dir.path().join("frank.db");

    let blob_values: Vec<String> = TEXT_CASES
        .iter()
        .map(|(_, hex)| format!("(X'{hex}')"))
        .collect();
    let insert_blobs = format!("INSERT INTO b(v) VALUES {}", blob_values.join(", "));
    let create = "CREATE TABLE b(v BLOB); CREATE TABLE t(x TEXT)";
    let insert_select = "INSERT INTO t(x) SELECT CAST(v AS TEXT) FROM b ORDER BY rowid";
    let readback = "SELECT hex(CAST(x AS BLOB)) FROM t ORDER BY rowid";

    // Stock oracle.
    {
        let conn = rusqlite::Connection::open(&stock_path).expect("stock open");
        conn.execute_batch(&format!("PRAGMA encoding = '{encoding}';"))
            .expect("stock PRAGMA");
        conn.execute_batch(create).expect("stock create");
        conn.execute_batch(&insert_blobs).expect("stock insert blobs");
        conn.execute_batch(insert_select).expect("stock insert-select");
    }
    let oracle = stock_read_col0_text(&stock_path, readback);
    assert_eq!(
        header_text_encoding(&stock_path),
        expect_header,
        "[{encoding}] stock header encoding"
    );

    // Frank under test.
    {
        let conn = Connection::open(frank_path.to_str().unwrap())
            .await
            .expect("frank open");
        conn.execute(&format!("PRAGMA encoding = '{encoding}';"))
            .await
            .expect("frank PRAGMA");
        conn.execute(create).await.expect("frank create");
        conn.execute(&insert_blobs).await.expect("frank insert blobs");
        conn.execute(insert_select)
            .await
            .expect("frank insert-select");
        conn.close().await.expect("frank close/flush");
    }
    assert_eq!(
        header_text_encoding(&frank_path),
        expect_header,
        "[{encoding}] frank header encoding"
    );
    let frank_via_stock = stock_read_col0_text(&frank_path, readback);

    for (i, (name, hex)) in TEXT_CASES.iter().enumerate() {
        eprintln!(
            "[{encoding}] {name:<26} X'{hex:<8}': stock={:<18} frank={}",
            oracle[i], frank_via_stock[i]
        );
    }
    assert_eq!(
        frank_via_stock, oracle,
        "[{encoding}] bd-f3s2l: VDBE SELECT-cast blob->text must byte-relabel like stock"
    );
}

#[test]
fn bd_f3s2l_vdbe_select_cast_blob_text_parity() {
    asupersync::test_utils::run_test(|| async {
        run_vdbe_text_pair("UTF-16le", 2).await;
        run_vdbe_text_pair("UTF-16be", 3).await;
        // UTF-8 control: byte-preserving path must stay byte-for-byte identical.
        run_vdbe_text_pair("UTF-8", 1).await;
    });
}

/// Numeric-cast blobs are encoding-specific: the SAME number is different bytes
/// under UTF-8 vs UTF-16. `(label, blob_hex, cast_type)`.
fn numeric_cases(encoding: &str) -> Vec<(&'static str, String, &'static str)> {
    // "12" and "3.5" encoded in the target DB encoding.
    let (twelve, three_five): (String, String) = match encoding {
        "UTF-16le" => ("31003200".into(), "33002E003500".into() /* 3 . 5 LE */),
        "UTF-16be" => ("00310032".into(), "0033002E0035".into()),
        _ => ("3132".into(), "332E35".into()),
    };
    vec![
        ("int_12", twelve.clone(), "INTEGER"),
        ("real_3_5", three_five, "REAL"),
        ("int_from_realblob", twelve, "REAL"),
    ]
}

fn frank_scalar(row: &fsqlite_core::connection::Row) -> String {
    match &row.values()[0] {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => s.to_string(),
        SqliteValue::Blob(b) => format!("blob:{}", b.len()),
    }
}

async fn run_numeric_pair(encoding: &str) {
    let dir = tempfile::TempDir::new().unwrap();
    let stock_path = dir.path().join("stock.db");
    let frank_path = dir.path().join("frank.db");
    let cases = numeric_cases(encoding);

    // Build a table holding each numeric blob (stored verbatim).
    let blob_values: Vec<String> = cases.iter().map(|(_, hex, _)| format!("(X'{hex}')")).collect();
    let insert_blobs = format!("INSERT INTO b(v) VALUES {}", blob_values.join(", "));
    let create = "CREATE TABLE b(v BLOB)";

    {
        let conn = rusqlite::Connection::open(&stock_path).expect("stock open");
        conn.execute_batch(&format!("PRAGMA encoding = '{encoding}';"))
            .expect("stock PRAGMA");
        conn.execute_batch(create).expect("stock create");
        conn.execute_batch(&insert_blobs).expect("stock insert");
    }
    let stock = rusqlite::Connection::open(&stock_path).expect("stock reopen");

    let conn = Connection::open(frank_path.to_str().unwrap())
        .await
        .expect("frank open");
    conn.execute(&format!("PRAGMA encoding = '{encoding}';"))
        .await
        .expect("frank PRAGMA");
    conn.execute(create).await.expect("frank create");
    conn.execute(&insert_blobs).await.expect("frank insert");

    for (idx, (label, _hex, ty)) in cases.iter().enumerate() {
        let q = format!("SELECT CAST(v AS {ty}) FROM b WHERE rowid = {}", idx + 1);
        let stock_val: String = stock
            .query_row(&q, [], |r| match r.get_ref(0).unwrap() {
                rusqlite::types::ValueRef::Null => Ok("NULL".to_owned()),
                rusqlite::types::ValueRef::Integer(n) => Ok(n.to_string()),
                rusqlite::types::ValueRef::Real(f) => Ok(format!("{f}")),
                rusqlite::types::ValueRef::Text(t) => Ok(String::from_utf8_lossy(t).into_owned()),
                rusqlite::types::ValueRef::Blob(b) => Ok(format!("blob:{}", b.len())),
            })
            .expect("stock numeric cast");
        let rows = conn.query(&q).await.expect("frank numeric cast");
        assert_eq!(rows.len(), 1, "[{encoding}/{label}] one row");
        let frank_val = frank_scalar(&rows[0]);
        eprintln!("[{encoding}] {label:<20} CAST AS {ty:<8}: stock={stock_val:<8} frank={frank_val}");
        assert_eq!(
            frank_val, stock_val,
            "[{encoding}/{label}] bd-f3s2l: CAST(blob AS {ty}) must parse via DB encoding like stock"
        );
    }
    conn.close().await.expect("frank close");
}

#[test]
fn bd_f3s2l_cast_blob_numeric_parity() {
    asupersync::test_utils::run_test(|| async {
        run_numeric_pair("UTF-16le").await;
        run_numeric_pair("UTF-16be").await;
        run_numeric_pair("UTF-8").await;
    });
}
