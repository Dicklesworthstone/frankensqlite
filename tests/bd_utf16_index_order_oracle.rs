#![recursion_limit = "512"]

//! UTF-16 index-order conformance for two second-pass encoding defects:
//!
//! * **bd-oqglk (P0)**: a multi-column TEXT index equality-prefix scan on a
//!   UTF-16 database returned EMPTY. The IdxGT/GE/LT/LE general branch decoded
//!   the probe key canonically but the cursor's current key raw, so
//!   `compare_index_prefix_keys` mis-compared and terminated the scan on its
//!   first row. Verified differentially vs rusqlite (bundled SQLite) across all
//!   three encodings.
//! * **bd-nmd19 (P1)**: the unique-index blind-append fast path compared its
//!   ascending guard under the DB text encoding while both operands were raw
//!   decodes, disagreeing with the b-tree's raw byte order and appending a key
//!   out of order — an image stock's `integrity_check` flags as a non-unique
//!   autoindex entry. Verified by reopening the frank-written image in stock.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn text_of(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Text(s) => s.as_ref().to_owned(),
        SqliteValue::Integer(i) => i.to_string(),
        SqliteValue::Null => "<NULL>".to_owned(),
        other => format!("{other:?}"),
    }
}

// ---------------------------------------------------------------------------
// bd-oqglk: multi-column index equality-prefix scan across encodings.
// ---------------------------------------------------------------------------

async fn frank_scan(enc: &str) -> (Vec<String>, i64) {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("f.db").to_string_lossy().into_owned();
    let conn = Connection::open(&db).await.unwrap();
    conn.execute(&format!("PRAGMA encoding = '{enc}';"))
        .await
        .unwrap();
    conn.execute("CREATE TABLE t(a TEXT, b TEXT, c TEXT);")
        .await
        .unwrap();
    conn.execute("CREATE INDEX i_t ON t(a,b,c);").await.unwrap();
    for (a, b, c) in [
        ("x", "y", "p"),
        ("x", "y", "q"),
        ("x", "z", "r"),
        ("w", "k", "s"),
    ] {
        conn.execute(&format!("INSERT INTO t VALUES('{a}','{b}','{c}');"))
            .await
            .unwrap();
    }
    let rows = conn
        .query("SELECT c FROM t WHERE a='x' AND b='y' AND c>'' ORDER BY c;")
        .await
        .unwrap();
    let scanned: Vec<String> = rows.iter().map(|r| text_of(&r.values()[0])).collect();
    let cnt = conn
        .query("SELECT count(*) FROM t WHERE a='x' AND b='y';")
        .await
        .unwrap();
    let count = match &cnt[0].values()[0] {
        SqliteValue::Integer(n) => *n,
        other => panic!("count returned {other:?}"),
    };
    conn.close().await.unwrap();
    (scanned, count)
}

#[test]
fn bd_oqglk_multicol_index_scan_utf16le() {
    asupersync::test_utils::run_test(|| async {
        let (scanned, count) = frank_scan("UTF-16le").await;
        assert_eq!(
            scanned,
            ["p", "q"],
            "UTF-16le multi-col scan must return [p,q]"
        );
        assert_eq!(count, 2, "UTF-16le two-col equality count must be 2");
    });
}

#[test]
fn bd_oqglk_multicol_index_scan_utf16be() {
    asupersync::test_utils::run_test(|| async {
        let (scanned, count) = frank_scan("UTF-16be").await;
        assert_eq!(
            scanned,
            ["p", "q"],
            "UTF-16be multi-col scan must return [p,q]"
        );
        assert_eq!(count, 2, "UTF-16be two-col equality count must be 2");
    });
}

#[test]
fn bd_oqglk_multicol_index_scan_utf8_control() {
    asupersync::test_utils::run_test(|| async {
        let (scanned, count) = frank_scan("UTF-8").await;
        assert_eq!(
            scanned,
            ["p", "q"],
            "UTF-8 control multi-col scan must return [p,q]"
        );
        assert_eq!(count, 2);
    });
}

// ---------------------------------------------------------------------------
// bd-nmd19: unique blind-append order must match the b-tree / stock.
// ---------------------------------------------------------------------------

#[test]
fn bd_nmd19_unique_blind_append_order_utf16le() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("u.db");
        let db = db_path.to_string_lossy().into_owned();
        let conn = Connection::open(&db).await.unwrap();
        conn.execute("PRAGMA encoding = 'UTF-16le';").await.unwrap();
        conn.execute("PRAGMA journal_mode = DELETE;").await.unwrap();
        conn.execute("CREATE TABLE u(k TEXT UNIQUE);")
            .await
            .unwrap();
        // Raw UTF-16LE byte order: 'a'(61 00) < 'b'(62 00) < U+A9C3(C3 A9) <
        // U+80C4(C4 80). Inserting U+80C4 BEFORE U+A9C3 exercises the append
        // guard: comparing raw operands under the DB encoding (the bug) mis-orders
        // this pair and appends out of order.
        for k in ["a", "b", "\u{80C4}", "\u{A9C3}"] {
            conn.execute(&format!("INSERT INTO u VALUES('{k}');"))
                .await
                .unwrap();
        }
        // Every key must still be findable through the unique index.
        for k in ["a", "b", "\u{80C4}", "\u{A9C3}"] {
            let rows = conn
                .query(&format!("SELECT count(*) FROM u WHERE k='{k}';"))
                .await
                .unwrap();
            assert_eq!(
                text_of(&rows[0].values()[0]),
                "1",
                "key U+{:04X} must be findable via the index",
                k.chars().next().unwrap() as u32
            );
        }
        conn.close().await.unwrap();

        // Stock SQLite must accept the frank-written image: an out-of-order
        // append surfaces as "non-unique entry in index sqlite_autoindex_u_1".
        let stock = rusqlite::Connection::open(&db_path).unwrap();
        let integ: String = stock
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(integ, "ok", "stock integrity_check on frank's UTF-16 image");
    });
}
